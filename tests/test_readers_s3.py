import shutil
import unittest
import mock
from exporters.readers.s3_reader import S3Reader

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


def get_keys_list(key_list):
    keys = []
    for key_name in key_list:
        keys.append(FakeKey(key_name))
    return keys


class S3ReaderTest(unittest.TestCase):
    def setUp(self):
        self.options = {
            'name': 'exporters.readers.s3_reader.S3Reader',
            'options': {
                'bucket': 'datasets.scrapinghub.com',
                'aws_access_key_id': 'AKIAJ6VP76KAK7UOUWEQ',
                'aws_secret_access_key': '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',
                'prefix': 'test_list/',
                'pattern': 'dump_p(.*)_US_(.*)'
            }
        }

        self.options_no_pattern = {
            'name': 'exporters.readers.s3_reader.S3Reader',
            'options': {
                'bucket': 'datasets.scrapinghub.com',
                'aws_access_key_id': 'AKIAJ6VP76KAK7UOUWEQ',
                'aws_secret_access_key': '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',
                'prefix': 'test_list/'
            }
        }

        self.options_no_prefix = {
            'name': 'exporters.readers.s3_reader.S3Reader',
            'options': {
                'bucket': 'datasets.scrapinghub.com',
                'aws_access_key_id': 'AKIAJ6VP76KAK7UOUWEQ',
                'aws_secret_access_key': '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',
                'pattern': '(.*)dump_p(.*)_US_(.*)'
            }
        }

    @mock.patch('boto.s3.bucket.Bucket.list', autospec=True)
    def test_list_no_keys(self, mock_bucket_list):
        mock_bucket_list.return_value = get_keys_list(NO_KEYS)
        reader = S3Reader(self.options)
        self.assertEqual([], reader.keys)
        shutil.rmtree(reader.tmp_folder, ignore_errors=True)

    @mock.patch('boto.s3.bucket.Bucket.list', autospec=True)
    def test_list_keys(self, mock_bucket_list):
        mock_bucket_list.return_value = get_keys_list(VALID_KEYS)
        reader = S3Reader(self.options)
        expected = ['test_list/dump_p1_US_a', 'test_list/dump_p1_US_b',
                    'test_list/dump_p2_US_a', 'test_list/dump_p_US_a']
        self.assertEqual(expected, reader.keys)
        shutil.rmtree(reader.tmp_folder, ignore_errors=True)

    @mock.patch('boto.s3.bucket.Bucket.list', autospec=True)
    def test_no_pattern_keys(self, mock_bucket_list):
        mock_bucket_list.return_value = get_keys_list(VALID_KEYS)
        reader = S3Reader(self.options_no_pattern)
        self.assertEqual(VALID_KEYS, reader.keys)
        shutil.rmtree(reader.tmp_folder, ignore_errors=True)

    @mock.patch('boto.s3.bucket.Bucket.list', autospec=True)
    def test_list_keys(self, mock_bucket_list):
        mock_bucket_list.return_value = get_keys_list(VALID_KEYS)
        reader = S3Reader(self.options_no_prefix)
        expected = ['test_list/dump_p1_US_a', 'test_list/dump_p1_US_b',
                    'test_list/dump_p2_US_a', 'test_list/dump_p_US_a']
        self.assertEqual(expected, reader.keys)
        shutil.rmtree(reader.tmp_folder, ignore_errors=True)
