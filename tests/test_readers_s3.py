import shutil
import unittest
import boto
import moto
from exporters.readers.s3_reader import S3Reader
from exporters.exceptions import ConfigurationError

NO_KEYS = ['test_list/test_key_1', 'test_list/test_key_2', 'test_list/test_key_3',
           'test_list/test_key_4', 'test_list/test_key_5', 'test_list/test_key_6',
           'test_list/test_key_7', 'test_list/test_key_8', 'test_list/test_key_9']

VALID_KEYS = ['test_list/dump_p1_US_a', 'test_list/dump_p1_UK_a', 'test_list/dump_p1_US_b',
              'test_list/dump_p2_US_a', 'test_list/dump_p1_ES_a', 'test_list/dump_p1_FR_a',
              'test_list/dump_p_US_a']


class FakeKey(object):
    def __init__(self, name):
        self.name = name
        self.key = name

    def get_contents_as_string(self):
        return self.name


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
            key.set_contents_from_string('')
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

        self.options_prefix_pointer = {
            'name': 'exporters.readers.s3_reader.S3Reader',
            'options': {
                'bucket': 'last_bucket',
                'aws_access_key_id': 'KEY',
                'aws_secret_access_key': 'SECRET',
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

    def test_prefix_pointer_list_keys(self):
        self.s3_conn.create_bucket('last_bucket')
        bucket = self.s3_conn.get_bucket('last_bucket')
        key = bucket.new_key('test_list/LAST')
        key.set_contents_from_string(VALID_KEYS[0])
        key.close()
        expected_prefix_pointer = VALID_KEYS[0]
        reader = S3Reader(self.options_prefix_pointer)
        self.assertEqual(expected_prefix_pointer, reader.prefixes[0])
        shutil.rmtree(reader.tmp_folder, ignore_errors=True)

    def test_prefix_pointer_list(self):
        self.s3_conn.create_bucket('last_bucket')
        bucket = self.s3_conn.get_bucket('last_bucket')
        key = bucket.new_key('test_list/LAST')
        pointers = ['pointer1', 'pointer2', 'pointer3']
        key.set_contents_from_string('\n'.join(pointers))
        key.close()
        reader = S3Reader(self.options_prefix_pointer)
        self.assertEqual(pointers, reader.prefixes)
        shutil.rmtree(reader.tmp_folder, ignore_errors=True)
