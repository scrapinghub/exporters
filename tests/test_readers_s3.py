import gzip
import json
import unittest
import StringIO
from contextlib import closing

import boto
import mock
import datetime

import dateparser
import moto
from exporters.readers.s3_reader import S3Reader, S3BucketKeysFetcher, get_bucket
from exporters.exceptions import ConfigurationError

from .utils import meta

NO_KEYS = ['test_list/test_key_1', 'test_list/test_key_2', 'test_list/test_key_3',
           'test_list/test_key_4', 'test_list/test_key_5', 'test_list/test_key_6',
           'test_list/test_key_7', 'test_list/test_key_8', 'test_list/test_key_9']

VALID_KEYS = ['test_list/dump_p1_US_a', 'test_list/dump_p1_UK_a', 'test_list/dump_p1_US_b',
              'test_list/dump_p2_US_a', 'test_list/dump_p1_ES_a', 'test_list/dump_p1_FR_a',
              'test_list/dump_p_US_a']


POINTER_KEYS = ['pointer1/dump_p1_US_a', 'pointer1/dump_p1_UK_a', 'pointer1/dump_p1_US_b',
                'pointer2/dump_p2_US_a', 'pointer2/dump_p1_ES_a', 'pointer2/dump_p1_FR_a',
                'pointer3/dump_p_US_a']


class FakeKey(object):
    def __init__(self, name):
        self.name = name
        self.key = name

    def get_contents_as_string(self):
        return json.dumps({'name': self.name})


def get_keys_list(key_list):
    keys = []
    for key_name in key_list:
        keys.append(FakeKey(key_name))
    return keys


class S3ReaderTest(unittest.TestCase):
    def setUp(self):
        self.mock_s3 = moto.mock_s3()
        self.mock_s3.start()
        self.s3_conn = boto.connect_s3()
        self.s3_conn.create_bucket('no_keys_bucket')
        bucket = self.s3_conn.get_bucket('no_keys_bucket')
        for key_name in NO_KEYS:
            key = bucket.new_key(key_name)
            key.set_contents_from_string('')
            key.close()

        self.s3_conn.create_bucket('valid_keys_bucket')
        bucket = self.s3_conn.get_bucket('valid_keys_bucket')

        for key_name in VALID_KEYS:
            key = bucket.new_key(key_name)
            out = StringIO.StringIO()
            with gzip.GzipFile(fileobj=out, mode='w') as f:
                f.write(json.dumps({'name': key_name}))
            key.set_contents_from_string(out.getvalue())
            key.close()

        self.options_no_keys = {
            'name': 'exporters.readers.s3_reader.S3Reader',
            'options': {
                'bucket': 'no_keys_bucket',
                'aws_access_key_id': 'KEY',
                'aws_secret_access_key': 'SECRET',
                'prefix': 'test_list/',
                'pattern': 'dump_p(.*)_US_(.*)'
            }
        }

        self.options_valid = {
            'name': 'exporters.readers.s3_reader.S3Reader',
            'options': {
                'bucket': 'valid_keys_bucket',
                'aws_access_key_id': 'KEY',
                'aws_secret_access_key': 'SECRET',
                'prefix': 'test_list/',
                'pattern': 'dump_p(.*)_US_(.*)'
            }
        }

        self.options_no_pattern = {
            'name': 'exporters.readers.s3_reader.S3Reader',
            'options': {
                'bucket': 'valid_keys_bucket',
                'aws_access_key_id': 'KEY',
                'aws_secret_access_key': 'SECRET',
                'prefix': 'test_list/',
                'batch_size': 1
            }
        }

        self.options_no_prefix = {
            'name': 'exporters.readers.s3_reader.S3Reader',
            'options': {
                'bucket': 'valid_keys_bucket',
                'aws_access_key_id': 'KEY',
                'aws_secret_access_key': 'SECRET',
                'pattern': '(.*)dump_p(.*)_US_(.*)'
            }
        }

        self.options_prefix_and_prefix_pointer = {
            'name': 'exporters.readers.s3_reader.S3Reader',
            'options': {
                'bucket': 'valid_keys_bucket',
                'aws_access_key_id': 'KEY',
                'aws_secret_access_key': 'SECRET',
                'prefix': 'test_list/',
                'prefix_pointer': 'test_list/LAST'
            }
        }

        self.options_date_prefix = {
            'name': 'exporters.readers.s3_reader.S3Reader',
            'options': {
                'bucket': 'valid_keys_bucket',
                'aws_access_key_id': 'KEY',
                'aws_secret_access_key': 'SECRET',
                'prefix': 'test_prefix/%Y-%m-%d'
            }
        }

        self.options_dateparser = {
            'name': 'exporters.readers.s3_reader.S3Reader',
            'options': {
                'bucket': 'valid_keys_bucket',
                'aws_access_key_id': 'KEY',
                'aws_secret_access_key': 'SECRET',
                'prefix': 'test_prefix/%Y-%m-%d',
                'prefix_format_using_date': 'yesterday'
            }
        }

        self.options_dateparser_range_3_days = {
            'name': 'exporters.readers.s3_reader.S3Reader',
            'options': {
                'bucket': 'valid_keys_bucket',
                'aws_access_key_id': 'KEY',
                'aws_secret_access_key': 'SECRET',
                'prefix': 'test_prefix/%Y-%m-%d',
                'prefix_format_using_date': ['2 days ago', 'today']
            }
        }

        self.options_date_prefix_list = {
            'name': 'exporters.readers.s3_reader.S3Reader',
            'options': {
                'bucket': 'valid_keys_bucket',
                'aws_access_key_id': 'KEY',
                'aws_secret_access_key': 'SECRET',
                'prefix': ['a_prefix/daily/%Y-%m-%d',
                           'b_prefix/daily/%Y-%m-%d',
                           'c_prefix/daily/%Y-%m-%d']
            }
        }

        self.options_prefix_list_using_date = {
            'name': 'exporters.readers.s3_reader.S3Reader',
            'options': {
                'bucket': 'valid_keys_bucket',
                'aws_access_key_id': 'KEY',
                'aws_secret_access_key': 'SECRET',
                'prefix': ['a_prefix/daily/%Y-%m-%d',
                           'b_prefix/daily/%Y-%m-%d',
                           'c_prefix/daily/%Y-%m-%d'],
                'prefix_format_using_date': 'yesterday'
            }
        }

        self.options_with_invalid_date_range = {
            'name': 'exporters.readers.s3_reader.S3Reader',
            'options': {
                'bucket': 'valid_keys_bucket',
                'aws_access_key_id': 'KEY',
                'aws_secret_access_key': 'SECRET',
                'prefix': 'test_prefix/%Y-%m-%d',
                'prefix_format_using_date': ['today', '2 days ago']
            }
        }

        self.options_valid_prefix = {
            'name': 'exporters.readers.s3_reader.S3Reader',
            'options': {
                'bucket': 's3://valid_keys_bucket',
                'aws_access_key_id': 'KEY',
                'aws_secret_access_key': 'SECRET',
                'prefix': 'test_list/',
                'pattern': 'dump_p(.*)_US_(.*)'
            }
        }

        self.options_valid_prefix_and_suffix = {
            'name': 'exporters.readers.s3_reader.S3Reader',
            'options': {
                'bucket': 's3://valid_keys_bucket/',
                'aws_access_key_id': 'KEY',
                'aws_secret_access_key': 'SECRET',
                'prefix': 'test_list/',
                'pattern': 'dump_p(.*)_US_(.*)'
            }
        }

    def tearDown(self):
        self.mock_s3.stop()

    def test_list_no_keys(self):
        reader = S3Reader(self.options_no_keys, meta())
        self.assertEqual([], reader.keys)

    def test_list_keys(self):
        reader = S3Reader(self.options_valid, meta())
        expected = ['test_list/dump_p1_US_a', 'test_list/dump_p1_US_b',
                    'test_list/dump_p2_US_a', 'test_list/dump_p_US_a']
        self.assertEqual(expected, reader.keys)

    def test_list_keys_prefix(self):
        reader = S3Reader(self.options_valid, meta())
        expected = ['test_list/dump_p1_US_a', 'test_list/dump_p1_US_b',
                    'test_list/dump_p2_US_a', 'test_list/dump_p_US_a']
        self.assertEqual(expected, reader.keys)

    def test_list_keys_prefix_and_suffix(self):
        reader = S3Reader(self.options_valid, meta())
        expected = ['test_list/dump_p1_US_a', 'test_list/dump_p1_US_b',
                    'test_list/dump_p2_US_a', 'test_list/dump_p_US_a']
        self.assertEqual(expected, reader.keys)

    def test_no_pattern_keys(self):
        reader = S3Reader(self.options_no_pattern, meta())
        expected = ['test_list/dump_p1_ES_a', 'test_list/dump_p1_FR_a',
                    'test_list/dump_p1_UK_a', 'test_list/dump_p1_US_a',
                    'test_list/dump_p1_US_b', 'test_list/dump_p2_US_a',
                    'test_list/dump_p_US_a']
        self.assertEqual(expected, reader.keys)

    def test_no_prefix_list_keys(self):
        reader = S3Reader(self.options_no_prefix, meta())
        expected = ['test_list/dump_p1_US_a', 'test_list/dump_p1_US_b',
                    'test_list/dump_p2_US_a', 'test_list/dump_p_US_a']
        self.assertEqual(expected, reader.keys)

    def test_prefix_and_prefix_pointer_list_keys(self):
        self.assertRaises(ConfigurationError, S3Reader,
                          self.options_prefix_and_prefix_pointer, meta())

    def test_get_batch(self):
        reader = S3Reader(self.options_no_pattern, meta())
        reader.set_last_position(None)
        batch = list(reader.get_next_batch())
        expected_batch = [{u'name': u'test_list/dump_p1_ES_a'}]
        self.assertEqual(batch, expected_batch)

    def test_date_prefix(self):
        reader = S3Reader(self.options_date_prefix, meta())
        expected = [datetime.datetime.now().strftime('test_prefix/%Y-%m-%d')]
        self.assertEqual(expected, reader.keys_fetcher.prefixes)

    def test_date_prefix_yesterday(self):
        reader = S3Reader(self.options_dateparser, meta())
        yesterday = dateparser.parse('yesterday').strftime('%Y-%m-%d')
        expected = ['test_prefix/{yesterday}'.format(yesterday=yesterday)]
        self.assertEqual(expected, reader.keys_fetcher.prefixes)

    def test_date_range_prefixes(self):
        reader = S3Reader(self.options_dateparser_range_3_days, meta())
        expected = ['test_prefix/{}'.format(dateparser.parse('2 days ago').strftime('%Y-%m-%d')),
                    'test_prefix/{}'.format(dateparser.parse('yesterday').strftime('%Y-%m-%d')),
                    'test_prefix/{}'.format(dateparser.parse('today').strftime('%Y-%m-%d'))]
        self.assertEqual(expected, reader.keys_fetcher.prefixes)

    def test_date_prefix_list(self):
        reader = S3Reader(self.options_date_prefix_list, meta())
        today = datetime.datetime.now().strftime('%Y-%m-%d')
        expected = ['a_prefix/daily/{}'.format(today),
                    'b_prefix/daily/{}'.format(today),
                    'c_prefix/daily/{}'.format(today)]
        self.assertEqual(expected, reader.keys_fetcher.prefixes)

    def test_prefix_list_using_date(self):
        reader = S3Reader(self.options_prefix_list_using_date, meta())
        yesterday = dateparser.parse('yesterday').strftime('%Y-%m-%d')
        expected = ['a_prefix/daily/{}'.format(yesterday),
                    'b_prefix/daily/{}'.format(yesterday),
                    'c_prefix/daily/{}'.format(yesterday)]
        self.assertEqual(expected, reader.keys_fetcher.prefixes)

    def test_get_read_streams(self):
        with closing(S3Reader(self.options_valid, meta())) as reader:
            file_names = set(['test_list/dump_p1_US_a', 'test_list/dump_p1_US_b',
                              'test_list/dump_p2_US_a', 'test_list/dump_p_US_a'])
            streams = list(reader.get_read_streams())
            for stream_data, file_name in zip(streams, file_names):
                file_obj, name, size = stream_data
                assert name in file_names
                file_names.remove(name)
            assert file_names == set()

    def test_invalid_date_range(self):
        self.assertRaisesRegexp(ConfigurationError,
                                'The end date should be greater or equal to '
                                'the start date for the '
                                'prefix_format_using_date option',
                                S3Reader,
                                self.options_with_invalid_date_range, meta())

    def test_read_compressed_file(self):
        self.s3_conn.create_bucket('compressed_files')
        bucket = self.s3_conn.get_bucket('compressed_files')
        key = bucket.new_key('test/dummy_data.gz')
        key.set_contents_from_filename('tests/data/dummy_data.jl.gz')
        key.close()

        options = {
            'name': 'exporters.readers.s3_reader.S3Reader',
            'options': {
                'bucket': 'compressed_files',
                'aws_access_key_id': 'KEY',
                'aws_secret_access_key': 'SECRET',
                'prefix': 'test/',
                'pattern': 'dummy_data(.*)'
            }
        }

        reader = S3Reader(options, meta())
        reader.set_last_position(None)
        batch = reader.get_next_batch()
        self.assertEqual(len(list(batch)), 200, 'Wrong items number read')


class TestS3BucketKeysFetcher(unittest.TestCase):

    def setUp(self):
        self.mock_s3 = moto.mock_s3()
        self.mock_s3.start()
        self.s3_conn = boto.connect_s3()
        self.s3_conn.create_bucket('last_bucket')
        bucket = self.s3_conn.get_bucket('last_bucket')
        key = bucket.new_key('test_list/LAST')
        self.pointers = ['pointer1', 'pointer2', 'pointer3', '']
        key.set_contents_from_string('\r\n'.join(self.pointers))
        key.close()

        for key_name in POINTER_KEYS:
            key = bucket.new_key(key_name)
            out = StringIO.StringIO()
            with gzip.GzipFile(fileobj=out, mode='w') as f:
                f.write(json.dumps({'name': key_name}))
            key.set_contents_from_string(out.getvalue())
            key.close()

        self.options_prefix_pointer = {
            'bucket': 'last_bucket',
            'aws_access_key_id': 'KEY',
            'aws_secret_access_key': 'SECRET',
            'prefix_pointer': 'test_list/LAST'
        }

    def test_prefix_pointer_list(self):
        self.s3_conn.create_bucket('last_bucket')
        expected_pointers = ['pointer1', 'pointer2', 'pointer3']
        fetcher = S3BucketKeysFetcher(self.options_prefix_pointer, 'KEY', 'SECRET')
        self.assertEqual(expected_pointers, fetcher.prefixes)

    def test_prefix_pointer_keys_list(self):
        fetcher = S3BucketKeysFetcher(self.options_prefix_pointer, 'KEY', 'SECRET')
        self.assertEqual(set(POINTER_KEYS), set(fetcher.pending_keys()))


class GetBucketTest(unittest.TestCase):
    def setUp(self):
        self.mock_s3 = moto.mock_s3()
        self.mock_s3.start()
        self.s3_conn = boto.connect_s3()
        self.s3_conn.create_bucket('fake_bucket')

    def tearDown(self):
        self.mock_s3.stop()

    @mock.patch('boto.s3.connection.S3Connection.get_bucket')
    def test_get_bucket_with_limited_access(self, mock_get_bucket):
        import boto.s3.bucket

        def reject_validated_get_bucket(*args, **kwargs):
            if kwargs.get('validate', True):
                raise boto.exception.S3ResponseError("Fake Error", "Permission Denied")

            bucket = mock.Mock(spec=boto.s3.bucket.Bucket)
            bucket.name = 'bucket_name'
            return bucket

        mock_get_bucket.side_effect = reject_validated_get_bucket

        get_bucket('some_bucket', 'fake-access-key', 'fake-secret-key')
