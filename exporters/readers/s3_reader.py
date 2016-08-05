import six
import httplib
import re
import datetime
from six.moves.urllib.request import urlopen
from exporters.readers.base_stream_reader import StreamBasedReader
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

        prefixes = [prefix] if isinstance(prefix, six.string_types) else prefix
        if prefix_pointer:
            prefixes = self._fetch_prefixes_from_pointer(prefix_pointer)
        return prefixes

    def _get_prefix_formatting_dates(self, prefix_dates):
        if not prefix_dates or isinstance(prefix_dates, six.string_types):
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


class S3Reader(StreamBasedReader):
    """
    Reads items from keys located in S3 buckets and compressed with gzip with a common path.

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
        'bucket': {'type': six.string_types},
        'aws_access_key_id': {
            'type': six.string_types,
            'env_fallback': 'EXPORTERS_S3READER_AWS_KEY'
        },
        'aws_secret_access_key': {
            'type': six.string_types,
            'env_fallback': 'EXPORTERS_S3READER_AWS_SECRET'
        },
        'prefix': {'type': six.string_types + (list,), 'default': ''},
        'prefix_pointer': {'type': six.string_types, 'default': None},
        'pattern': {'type': six.string_types, 'default': None},
        'prefix_format_using_date': {'type': six.string_types + (tuple, list), 'default': None}
    }

    def __init__(self, *args, **kwargs):
        super(S3Reader, self).__init__(*args, **kwargs)
        bucket_name = self.read_option('bucket')
        self.logger.info('Starting S3Reader for bucket: %s' % bucket_name)

        self.bucket = get_bucket(bucket_name,
                                 self.read_option('aws_access_key_id'),
                                 self.read_option('aws_secret_access_key'))

        self.keys_fetcher = S3BucketKeysFetcher(self.options,
                                                self.read_option('aws_access_key_id'),
                                                self.read_option('aws_secret_access_key'))
        self.keys = self.keys_fetcher.pending_keys()
        self.logger.info('S3Reader has been initiated')

    def open_stream(self, stream):
        return urlopen(self.bucket.get_key(stream.filename).generate_url(S3_URL_EXPIRES_IN))

    def get_read_streams(self):
        from exporters.bypasses.stream_bypass import Stream
        for key_name in self.keys:
            key = self.bucket.get_key(key_name)
            yield Stream(key_name, key.size, None)
