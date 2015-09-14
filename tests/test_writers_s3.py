import re
import unittest

import boto
import moto
import mock

from exporters.export_managers.settings import Settings
from exporters.records.base_record import BaseRecord
from exporters.writers.s3_writer import S3Writer


def create_fake_key():
    key = mock.Mock()
    return key


def create_fake_bucket():
    bucket = mock.Mock()
    bucket.new_key.side_effect = create_fake_key()
    return bucket


def create_fake_connection():
    connection = mock.Mock()
    connection.get_bucket.side_effect = create_fake_bucket()
    return connection


class S3WriterTest(unittest.TestCase):

    def setUp(self):
        self.mock_s3 = moto.mock_s3()
        self.mock_s3.start()
        self.s3_conn = boto.connect_s3()
        self.s3_conn.create_bucket('fake_bucket')

        exporter_options = {
            'log_level': 'DEBUG',
            'logger_name': 'export-pipeline',
        }
        self.settings = Settings(exporter_options)

    def tearDown(self):
        self.mock_s3.stop()

    def test_write_s3(self):
        # given
        data = [
            {'name': 'Roberto', 'birthday': '12/05/1987'},
            {'name': 'Claudia', 'birthday': '21/12/1985'},
        ]
        items_to_write = [BaseRecord(d) for d in data]
        options = self.get_writer_config()

        # when:
        writer = S3Writer(options, self.settings)
        writer.write_batch(items_to_write)
        writer.close_writer()

        # then:
        bucket = self.s3_conn.get_bucket('fake_bucket')
        saved_keys = [k for k in bucket.list()]
        self.assertEquals(1, len(saved_keys))
        self.assertTrue(re.match('tests/.*[.]gz', saved_keys[0].name))

    def get_writer_config(self):
        return {
            'name': 'exporters.writers.s3_writer.S3Writer',
            'options': {
                'bucket': 'fake_bucket',
                'aws_access_key_id': 'FAKE_ACCESS_KEY',
                'aws_secret_access_key': 'FAKE_SECRET_KEY',
                'filebase': 'tests',
            }
        }
