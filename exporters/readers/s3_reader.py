import gzip
import json
import os
import tempfile
import re
import datetime
from exporters.progress_callback import BotoDownloadProgress
from exporters.readers.base_reader import BaseReader
from exporters.records.base_record import BaseRecord
from exporters.default_retries import retry_long, retry_short
from exporters.exceptions import ConfigurationError
import logging


def get_bucket(bucket, aws_access_key_id, aws_secret_access_key, **kwargs):
    import boto

    if len(aws_access_key_id) > len(aws_secret_access_key):
        logging.warn("The AWS credential keys aren't in the usual size,"
                     " are you using the correct ones?")

    connection = boto.connect_s3(aws_access_key_id, aws_secret_access_key)
    try:
        return connection.get_bucket(bucket)
    except boto.exception.S3ResponseError:
        return connection.get_bucket(bucket, validate=False)


def format_prefixes(prefixes, start, end):
    import dateparser
    start_date = dateparser.parse(start)
    end_date = dateparser.parse(end)
    if start_date > end_date:
        raise ConfigurationError("Invalid date range")

    dates = []
    while start_date <= end_date:
        dates.append(start_date)
        start_date += datetime.timedelta(days=1)

    return [date.strftime(p) for date in dates for p in prefixes]


class S3BucketKeysFetcher(object):
    def __init__(self, reader_options, aws_access_key_id, aws_secret_access_key):
        self.source_bucket = get_bucket(reader_options.get('bucket'), aws_access_key_id, aws_secret_access_key)
        self.pattern = reader_options.get('pattern', None)

        prefix = reader_options.get('prefix', '')
        prefix_list = reader_options.get('prefix_list', '')
        prefix_pointer = reader_options.get('prefix_pointer', '')
        prefix_format_using_date = reader_options.get('prefix_format_using_date')
        prefix_format_using_date_start = reader_options.get('prefix_format_using_date_start')
        prefix_format_using_date_end = reader_options.get('prefix_format_using_date_end')

        prefixes = self._get_prefixes(prefix, prefix_list, prefix_pointer)
        start, end = self._get_prefix_formatting(prefix_format_using_date,
                                                 prefix_format_using_date_start,
                                                 prefix_format_using_date_end)

        self.prefixes = format_prefixes(prefixes, start, end)
        self.logger = logging.getLogger('s3-reader')
        self.logger.setLevel(logging.INFO)

    def _get_prefixes(self, prefix, prefix_list, prefix_pointer):
        if prefix and prefix_list:
            raise ConfigurationError("prefix and prefix_list options cannot be used together")
        if prefix and prefix_pointer:
            raise ConfigurationError("prefix and prefix_pointer options cannot be used together")
        if prefix_list and prefix_pointer:
            raise ConfigurationError("prefix_list and prefix_pointer options cannot be used together")

        if prefix:
            return [prefix]
        if prefix_list:
            return prefix_list
        if prefix_pointer:
            return self._fetch_prefixes_from_pointer(prefix_pointer)
        return ['']

    def _get_prefix_formatting(self, prefix_format_using_date,
                               prefix_format_using_date_start,
                               prefix_format_using_date_end):
        if prefix_format_using_date and (prefix_format_using_date_start or
                                         prefix_format_using_date_end):
            raise ConfigurationError("prefix_format_using_date and prefix_format_using_date_start or "
                                     "prefix_format_using_date_end options cannot be used together")

        if prefix_format_using_date:
            return prefix_format_using_date, prefix_format_using_date
        if not prefix_format_using_date_start:
            prefix_format_using_date_start = "today"
        if not prefix_format_using_date_end:
            prefix_format_using_date_end = "today"

        return prefix_format_using_date_start, prefix_format_using_date_end

    @retry_short
    def _download_pointer(self, prefix_pointer):
        return self.source_bucket.get_key(prefix_pointer).get_contents_as_string()

    def _fetch_prefixes_from_pointer(self, prefix_pointer):
        return filter(None, self._download_pointer(prefix_pointer).splitlines())

    def _get_keys_from_bucket(self):
        keys = []
        for prefix in self.prefixes:
            for key in self.source_bucket.list(prefix=prefix):
                if self.pattern:
                    if self._should_add_key(key):
                        keys.append(key.name)
                    else:
                        self.logger.info('Skipping S3 key {}. No match with pattern'.format(key.name))
                else:
                    keys.append(key.name)
        if self.pattern and not keys:
            self.logger.warn('No S3 keys found that match provided pattern: {}'.format(self.pattern))
        return keys

    def _should_add_key(self, key):
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
        'prefix_list': {'type': list, 'default': []},
        'pattern': {'type': basestring, 'default': None},
        'prefix_format_using_date': {'type': basestring, 'default': None},
        'prefix_format_using_date_start': {'type': basestring, 'default': None},
        'prefix_format_using_date_end': {'type': basestring, 'default': None},
    }

    def __init__(self, options):
        super(S3Reader, self).__init__(options)
        self.batch_size = self.read_option('batch_size')
        bucket_name = self.read_option('bucket')
        self.logger.info('Starting S3Reader for bucket: %s' % bucket_name)

        self.bucket = get_bucket(bucket_name,
                                 self.read_option('aws_access_key_id'),
                                 self.read_option('aws_secret_access_key'))

        self.keys_fetcher = S3BucketKeysFetcher(options['options'],
                                                self.read_option('aws_access_key_id'),
                                                self.read_option('aws_secret_access_key'))
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
        if not self.keys:
            self.finished = True
            return
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
