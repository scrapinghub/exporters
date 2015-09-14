import random
import unittest

from mock import Mock, patch

from exporters.export_managers.settings import Settings
from exporters.records.base_record import BaseRecord
from exporters.writers.s3_writer import S3Writer


def create_fake_key():
    key = Mock()
    return key


def create_fake_bucket():
    bucket = Mock()
    bucket.new_key.side_effect = create_fake_key()
    return bucket


def create_fake_connection():
    connection = Mock()
    connection.get_bucket.side_effect = create_fake_bucket()
    return connection


@patch('boto.connect_s3', autospec=True)
class S3WriterTest(unittest.TestCase):

    def setUp(self):
        self.options = {
            'exporter_options': {
                'log_level': 'DEBUG',
                'logger_name': 'export-pipeline'
            },
            'writer': {
                'name': 'exporters.writers.s3_writer.S3Writer',
                'options': {
                    'bucket': 'datasets.scrapinghub.com',
                    'aws_access_key_id': 'AKIAJDGLM4HBWQDMWPOQ',
                    'aws_secret_access_key': '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',
                    'filebase': 'tests',
                }
            }
        }
        self.settings = Settings(self.options['exporter_options'])

    def test_write_s3(self, conn_mock):
        conn_mock.return_value = create_fake_connection()
        writer = S3Writer(self.options['writer'], self.settings)
        items_to_write = []
        for i in range(0, 10):
            item = BaseRecord()
            item['key'] = i
            item['value'] = random.randint(0, 10000)
            items_to_write.append(item)

        writer.write_batch(items_to_write)
