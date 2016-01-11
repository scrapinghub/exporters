import gzip
import json
import os
import tempfile
import re
from exporters.progress_callback import BotoDownloadProgress
from exporters.readers.base_reader import BaseReader
from exporters.records.base_record import BaseRecord
from exporters.default_retries import retry_long
from exporters.exceptions import ConfigurationError
import logging


def get_bucket(bucket, aws_access_key_id, aws_secret_access_key, **kwargs):
    import boto

    if len(aws_access_key_id) > len(aws_secret_access_key):
        logging.warn("The AWS credential keys aren't in the usual size,"
                     " are you using the correct ones?")

    connection = boto.connect_s3(aws_access_key_id, aws_secret_access_key)
    return connection.get_bucket(bucket)


class S3BucketKeysFetcher(object):
    def __init__(self, reader_options):
        self.source_bucket = get_bucket(**reader_options)
        self.pattern = reader_options.get('pattern', None)
        single_prefix = reader_options.get('prefix', '')
        self.prefix_pointer = reader_options.get('prefix_pointer', '')
        if single_prefix and self.prefix_pointer:
            raise ConfigurationError("prefix and prefix_pointer options cannot be used together")
        self.prefixes = [single_prefix]
        if self.prefix_pointer:
            self.prefixes = self._fetch_prefixes_from_pointer(self.prefix_pointer)

    @retry_long
    def _download_pointer(self, prefix_pointer):
        return self.source_bucket.get_key(prefix_pointer).get_contents_as_string()

    def _fetch_prefixes_from_pointer(self, prefix_pointer):
        return [pointer for pointer in self._download_pointer(prefix_pointer).split('\n') if pointer]

    def _get_keys_from_bucket(self):
        keys = []
        for prefix in self.prefixes:
            for key in self.source_bucket.list(prefix=prefix):
                if self.pattern:
                    if self._should_add_key(key, prefix):
                        keys.append(key.name)
                else:
                    keys.append(key.name)
        return keys

    def _should_add_key(self, key, prefix):
        return bool(re.findall(self.pattern, key.name))

    def pending_keys(self):
        return self._get_keys_from_bucket()


class S3Reader(BaseReader):
    """
    Reads items from keys located in S3 buckets and compressed with gzip with a common path.

        - batch_size (int)
            Number of items to be returned in each batch

        - bucket (str)
            Name of the bucket to retrieve items from.

        - aws_access_key_id (str)
            Public access key to the s3 bucket.

        - aws_secret_access_key (str)
            Secret access key to the s3 bucket.

        - prefix (str)
            Prefix of s3 keys to be read.

        - prefix_pointer (str)
            Prefix pointing to the last version of dataset. This adds support for regular exports.
            For example:
                We have a weekly export set with CRON. If we wanted to point to a new data
                prefix every week, we should keep updating the export configuration. With a pointer,
                we can set the reader to read from that key, which contains one or several
                lines with valid prefixes to datasets, so only that pointer file should be updated.

        - pattern (str)
            S3 key name pattern (REGEX). All files that don't match this regex string will be
            discarded by the reader.

    """

    # List of options to set up the reader
    supported_options = {
        'batch_size': {'type': int, 'default': 10000},
        'bucket': {'type': basestring},
        'aws_access_key_id': {'type': basestring, 'env_fallback': 'EXPORTERS_S3READER_AWS_KEY'},
        'aws_secret_access_key': {'type': basestring, 'env_fallback': 'EXPORTERS_S3READER_AWS_SECRET'},
        'prefix': {'type': basestring, 'default': ''},
        'prefix_pointer': {'type': basestring, 'default': None},
        'pattern': {'type': basestring, 'default': None}
    }

    def __init__(self, options):
        super(S3Reader, self).__init__(options)
        self.batch_size = self.read_option('batch_size')
        bucket_name = self.read_option('bucket')
        self.logger.info('Starting S3Reader for bucket: %s' % bucket_name)

        self.bucket = get_bucket(bucket_name,
                                 self.read_option('aws_access_key_id'),
                                 self.read_option('aws_secret_access_key'))

        single_prefix = self.read_option('prefix')
        self.prefix_pointer = self.read_option('prefix_pointer')
        self.pattern = self.read_option('pattern')

        if single_prefix and self.prefix_pointer:
            raise ConfigurationError("prefix and prefix_pointer options cannot be used together")

        self.keys_fetcher = S3BucketKeysFetcher(options['options'])
        self.keys = self.keys_fetcher.pending_keys()
        self.read_keys = []
        self.current_key = None
        self.last_line = 0
        self.logger.info('S3Reader has been initiated')
        self.tmp_folder = tempfile.mkdtemp()

    @retry_long
    def get_key(self, file_path, progress):
        """
        Downloads and stores an s3 key
        """
        self.logger.info('Downloading key: %s' % self.current_key)
        self.bucket.get_key(self.current_key).get_contents_to_filename(file_path, cb=progress)

    def get_next_batch(self):
        """
        This method is called from the manager. It must return a list or a generator of BaseRecord objects.
        When it has nothing else to read, it must set class variable "finished" to True.
        """
        file_path = '{}/ds_dump.gz'.format(self.tmp_folder)
        if not self.current_key:
            progress = BotoDownloadProgress(self.logger)
            self.current_key = self.keys[0]
            self.get_key(file_path, progress)
            self.last_line = 0

        dump_file = gzip.open(file_path, 'r')
        lines = dump_file.readlines()
        if self.last_line + self.batch_size <= len(lines):
            last_item = self.last_line + self.batch_size
        else:
            last_item = len(lines)
            self.read_keys.append(self.current_key)
            self.keys.remove(self.current_key)
            self.current_key = None
            if len(self.keys) == 0:
                self.finished = True
                os.remove(file_path)
        for line in lines[self.last_line:last_item]:
            line = line.replace("\n", '')
            item = BaseRecord(json.loads(line))
            self.stats['read_items'] += 1
            yield item
        dump_file.close()

        self.last_line += self.batch_size

        self.last_position['keys'] = self.keys
        self.last_position['read_keys'] = self.read_keys
        self.last_position['current_key'] = self.current_key
        self.last_position['last_line'] = self.last_line
        self.logger.debug('Done reading batch')

    def set_last_position(self, last_position):
        """
        Called from the manager, it is in charge of updating the last position of data commited by the writer, in order to
        have resume support
        """
        if last_position is None:
            self.last_position = {}
            self.last_position['keys'] = self.keys
            self.last_position['read_keys'] = self.read_keys
            self.last_position['current_key'] = None
            self.last_position['last_line'] = 0
        else:
            self.last_position = last_position
            self.keys = self.last_position['keys']
            self.read_keys = self.last_position['read_keys']
            file_path = '{}/ds_dump.gz'.format(self.tmp_folder)
            if self.last_position['current_key']:
                self.current_key = self.last_position['current_key']
            else:
                self.current_key = self.keys[0]
                self.bucket.get_key(self.current_key).get_contents_to_filename(file_path)
                self.last_line = 0
            self.last_line = self.last_position['last_line']
