import httplib
import json
import tempfile
import re
import datetime
import shutil
import zlib
from six.moves.urllib.request import urlopen
from exporters.readers.base_reader import BaseReader
from exporters.records.base_record import BaseRecord
from exporters.default_retries import retry_short
from exporters.exceptions import ConfigurationError, InvalidDateRangeError
import logging

from exporters.utils import get_bucket_name

S3_URL_EXPIRES_IN = 1800  # half an hour should be enough


def patch_http_response_read(func):
    def inner(*args):
        try:
            return func(*args)
        except httplib.IncompleteRead, e:
            return e.partial

    return inner
httplib.HTTPResponse.read = patch_http_response_read(httplib.HTTPResponse.read)


def get_bucket(bucket, aws_access_key_id, aws_secret_access_key, **kwargs):
    import boto

    bucket = get_bucket_name(bucket)

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
    start_date = dateparser.parse(start or 'today')
    end_date = dateparser.parse(end or 'today')
    if start_date > end_date:
        raise InvalidDateRangeError

    dates = []
    while start_date <= end_date:
        dates.append(start_date)
        start_date += datetime.timedelta(days=1)

    return [date.strftime(p) for date in dates for p in prefixes]


@retry_short
def read_chunk(key):
    return key.read(1024 * 1024)


def create_decompressor():
    # create zlib decompressor enabling automatic header detection:
    # See: http://stackoverflow.com/a/22310760/149872
    AUTOMATIC_HEADER_DETECTION_MASK = 32
    return zlib.decompressobj(AUTOMATIC_HEADER_DETECTION_MASK | zlib.MAX_WBITS)


def stream_decompress_multi(key):
    dec = create_decompressor()
    while True:
        chunk = read_chunk(key)
        if not chunk:
            break
        rv = dec.decompress(chunk)
        if rv:
            yield rv
        if dec.unused_data:
            unused = dec.unused_data
            while unused:
                dec = create_decompressor()
                rv = dec.decompress(unused)
                if rv:
                    yield rv
                unused = dec.unused_data


class S3BucketKeysFetcher(object):
    def __init__(self, reader_options, aws_access_key_id, aws_secret_access_key):
        self.source_bucket = get_bucket(
            reader_options.get('bucket'), aws_access_key_id, aws_secret_access_key)
        self.pattern = reader_options.get('pattern', None)

        prefix = reader_options.get('prefix', '')
        prefix_pointer = reader_options.get('prefix_pointer', '')
        prefix_format_using_date = reader_options.get('prefix_format_using_date')

        unformatted_prefixes = self._get_prefixes(prefix, prefix_pointer)
        try:
            start, end = self._get_prefix_formatting_dates(prefix_format_using_date)
        except ValueError:
            raise ConfigurationError('The option prefix_format_using_date '
                                     'should be either a date string or two '
                                     'date strings in a list/tuple')
        try:
            self.prefixes = format_prefixes(unformatted_prefixes, start, end)
        except InvalidDateRangeError:
            raise ConfigurationError('The end date should be greater or equal '
                                     'to the start date for the '
                                     'prefix_format_using_date option')

        self.logger = logging.getLogger('s3-reader')
        self.logger.setLevel(logging.INFO)

    def _get_prefixes(self, prefix, prefix_pointer):
        if prefix and prefix_pointer:
            raise ConfigurationError('prefix and prefix_pointer options '
                                     'cannot be used together')

        prefixes = [prefix] if isinstance(prefix, basestring) else prefix
        if prefix_pointer:
            prefixes = self._fetch_prefixes_from_pointer(prefix_pointer)
        return prefixes

    def _get_prefix_formatting_dates(self, prefix_dates):
        if not prefix_dates or isinstance(prefix_dates, basestring):
            prefix_dates = (prefix_dates, prefix_dates)
        return prefix_dates

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
                        self.logger.info(
                            'Skipping S3 key {}. No match with pattern'.format(key.name))
                else:
                    keys.append(key.name)
        if self.pattern and not keys:
            self.logger.warn(
                'No S3 keys found that match provided pattern: {}'.format(self.pattern))
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
        'aws_access_key_id': {
            'type': basestring,
            'env_fallback': 'EXPORTERS_S3READER_AWS_KEY'
        },
        'aws_secret_access_key': {
            'type': basestring,
            'env_fallback': 'EXPORTERS_S3READER_AWS_SECRET'
        },
        'prefix': {'type': (basestring, list), 'default': ''},
        'prefix_pointer': {'type': basestring, 'default': None},
        'pattern': {'type': basestring, 'default': None},
        'prefix_format_using_date': {'type': (basestring, tuple, list), 'default': None}
    }

    def __init__(self, *args, **kwargs):
        super(S3Reader, self).__init__(*args, **kwargs)
        self.batch_size = self.read_option('batch_size')
        bucket_name = self.read_option('bucket')
        self.logger.info('Starting S3Reader for bucket: %s' % bucket_name)

        self.bucket = get_bucket(bucket_name,
                                 self.read_option('aws_access_key_id'),
                                 self.read_option('aws_secret_access_key'))

        self.keys_fetcher = S3BucketKeysFetcher(self.options,
                                                self.read_option('aws_access_key_id'),
                                                self.read_option('aws_secret_access_key'))
        self.keys = self.keys_fetcher.pending_keys()
        self.read_keys = []
        self.current_key = None
        self.last_block = 0
        self.logger.info('S3Reader has been initiated')
        self.tmp_folder = tempfile.mkdtemp()
        self.lines_reader = self.read_lines_from_keys()

    def get_read_streams(self):
        from exporters.bypasses.stream_bypass import Stream
        for key_name in self.keys:
            key = self.bucket.get_key(key_name)
            file_obj = urlopen(key.generate_url(S3_URL_EXPIRES_IN))
            yield Stream(file_obj, key_name, key.size)

    def read_lines_from_keys(self):
        for current_key in self.keys:
            self.current_key = current_key
            self.last_position['current_key'] = current_key
            key = self.bucket.get_key(current_key)
            self.last_leftover = ''
            index_block = 0
            for uncompressed in stream_decompress_multi(key):
                if index_block >= self.last_block:
                    block_text = self.last_leftover + uncompressed
                    items = block_text.split('\n')
                    for i in items:
                        if i:
                            try:
                                obj = json.loads(i)
                            except ValueError:
                                # Last uncomplete line
                                self.last_leftover = i
                                self.last_position['last_leftover'] = self.last_leftover
                            else:
                                item = BaseRecord(obj)
                                self.last_leftover = ''
                                self.last_position['last_leftover'] = self.last_leftover
                                yield item
                    self.last_block += 1
                index_block += 1
            self.read_keys.append(current_key)
            self.current_key = None
            self.last_position['keys'].remove(current_key)
            self.last_position['read_keys'] = self.read_keys
            self.last_position['current_key'] = self.current_key
            self.last_position['last_block'] = self.last_block
            self.last_block = 0
        self.finished = True

    def get_next_batch(self):
        """
        This method is called from the manager. It must return a list or a generator
        of BaseRecord objects.
        When it has nothing else to read, it must set class variable "finished" to True.
        """
        count = 0
        while count < self.batch_size:
            count += 1
            yield next(self.lines_reader)
        self.logger.debug('Done reading batch')

    def set_last_position(self, last_position):
        """
        Called from the manager, it is in charge of updating the last position of data commited
        by the writer, in order to have resume support
        """
        if last_position is None:
            self.last_position = {}
            self.last_position['keys'] = list(self.keys)
            self.last_position['read_keys'] = self.read_keys
            self.last_position['current_key'] = None
            self.last_position['last_block'] = 0
        else:
            self.last_position = last_position
            self.keys = self.last_position['keys']
            self.read_keys = self.last_position['read_keys']
            if self.last_position['current_key']:
                self.current_key = self.last_position['current_key']
            else:
                self.current_key = self.keys[0]
                self.last_block = 0
            self.last_block = self.last_position['last_block']

    def close(self):
        shutil.rmtree(self.tmp_folder)
