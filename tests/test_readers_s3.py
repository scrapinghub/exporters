import gzip
import json
import shutil
import unittest
import StringIO
import boto
import moto
from exporters.readers.s3_reader import S3Reader, S3BucketKeysFetcher
from exporters.exceptions import ConfigurationError

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
                'prefix': 'test_list/'
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

    def tearDown(self):
        self.mock_s3.stop()

    def test_list_no_keys(self):
        reader = S3Reader(self.options_no_keys)
        self.assertEqual([], reader.keys)
        shutil.rmtree(reader.tmp_folder, ignore_errors=True)

    def test_list_keys(self):
        reader = S3Reader(self.options_valid)
        expected = ['test_list/dump_p1_US_a', 'test_list/dump_p1_US_b',
                    'test_list/dump_p2_US_a', 'test_list/dump_p_US_a']
        self.assertEqual(expected, reader.keys)
        shutil.rmtree(reader.tmp_folder, ignore_errors=True)

    def test_no_pattern_keys(self):
        reader = S3Reader(self.options_no_pattern)
        expected = ['test_list/dump_p1_ES_a', 'test_list/dump_p1_FR_a',
                    'test_list/dump_p1_UK_a', 'test_list/dump_p1_US_a',
                    'test_list/dump_p1_US_b', 'test_list/dump_p2_US_a',
                    'test_list/dump_p_US_a']
        self.assertEqual(expected, reader.keys)
        shutil.rmtree(reader.tmp_folder, ignore_errors=True)

    def test_no_prefix_list_keys(self):
        reader = S3Reader(self.options_no_prefix)
        expected = ['test_list/dump_p1_US_a', 'test_list/dump_p1_US_b',
                    'test_list/dump_p2_US_a', 'test_list/dump_p_US_a']
        self.assertEqual(expected, reader.keys)
        shutil.rmtree(reader.tmp_folder, ignore_errors=True)

    def test_prefix_and_prefix_pointer_list_keys(self):
        self.assertRaises(ConfigurationError, S3Reader,
                          self.options_prefix_and_prefix_pointer)

    def test_get_batch(self):
        reader = S3Reader(self.options_no_pattern)
        reader.set_last_position(None)
        batch = list(reader.get_next_batch())
        expected_batch = [{u'name': u'test_list/dump_p1_ES_a'}]
        self.assertEqual(batch, expected_batch)


class TestS3BucketKeysFetcher(unittest.TestCase):

    def setUp(self):
        self.mock_s3 = moto.mock_s3()
        self.mock_s3.start()
        self.s3_conn = boto.connect_s3()
        self.s3_conn.create_bucket('last_bucket')
        bucket = self.s3_conn.get_bucket('last_bucket')
        key = bucket.new_key('test_list/LAST')
        self.pointers = ['pointer1', 'pointer2', 'pointer3']
        key.set_contents_from_string('\n'.join(self.pointers))
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
        fetcher = S3BucketKeysFetcher(self.options_prefix_pointer)
        self.assertEqual(self.pointers, fetcher.prefixes)

    def test_prefix_pointer_keys_list(self):
        fetcher = S3BucketKeysFetcher(self.options_prefix_pointer)
        self.assertEqual(set(POINTER_KEYS), set(fetcher.pending_keys()))
