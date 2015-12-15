import json
import re
import tempfile
import unittest

import boto
import moto
import mock
from exporters.export_managers.s3_to_s3_bypass import S3Bypass
from exporters.exporter_config import ExporterConfig

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


class S3BypassTest(unittest.TestCase):

    def setUp(self):
        self.mock_s3 = moto.mock_s3()
        self.mock_s3.start()
        self.s3_conn = boto.connect_s3()
        self.s3_conn.create_bucket('source_bucket')

        self.source_bucket = self.s3_conn.get_bucket('source_bucket')
        self.data = [
            {'name': 'Roberto', 'birthday': '12/05/1987'},
            {'name': 'Claudia', 'birthday': '21/12/1985'},
        ]
        key = self.source_bucket.new_key('some_prefix/test_key')
        key.set_contents_from_string(json.dumps(self.data))


    def tearDown(self):
        self.mock_s3.stop()

    def test_copy_bypass_s3(self):
        # given
        self.s3_conn.create_bucket('dest_bucket')
        options = self.get_config()

        # when:
        bypass = S3Bypass(options)
        bypass.bypass()

        # then:
        bucket = self.s3_conn.get_bucket('dest_bucket')
        keys = list(bucket.list('some_prefix/'))
        self.assertEquals('some_prefix/test_key', keys[0].name)
        contents = json.loads(keys[0].get_contents_as_string())
        for item in contents:
            self.assertIn('name', item.keys())
            self.assertIn('birthday', item.keys())


    def test_upload_download_bypass_s3(self):
        # given
        self.s3_conn.create_bucket('dest_ud_bucket')
        options = self.get_ud_config()

        # when:
        bypass = S3Bypass(options)
        bypass.copy_mode = False
        bypass.tmp_folder = tempfile.mkdtemp()
        bypass.bypass()

        # then:
        bucket = self.s3_conn.get_bucket('dest_ud_bucket')
        keys = list(bucket.list('some_prefix/'))
        self.assertEquals('some_prefix/test_key', keys[0].name)
        contents = json.loads(keys[0].get_contents_as_string())
        for item in contents:
            self.assertIn('name', item.keys())
            self.assertIn('birthday', item.keys())


    def get_config(self):
        return ExporterConfig({
            'reader': {
                'name': 'exporters.readers.s3_reader.S3Reader',
                'options': {
                    'bucket': 'source_bucket',
                    'aws_access_key_id': 'a',
                    'aws_secret_access_key': 'a',
                    'prefix': 'some_prefix/'
                }
            },
            'writer': {
                'name': 'exporters.writers.s3_writer.S3Writer',
                'options': {
                    'bucket': 'dest_bucket',
                    'aws_access_key_id': 'b',
                    'aws_secret_access_key': 'b',
                    'filebase': 'some_prefix/'
                }
            }
        })

    def get_ud_config(self):
        return ExporterConfig({
            'reader': {
                'name': 'exporters.readers.s3_reader.S3Reader',
                'options': {
                    'bucket': 'source_bucket',
                    'aws_access_key_id': 'a',
                    'aws_secret_access_key': 'a',
                    'prefix': 'some_prefix/'
                }
            },
            'writer': {
                'name': 'exporters.writers.s3_writer.S3Writer',
                'options': {
                    'bucket': 'dest_ud_bucket',
                    'aws_access_key_id': 'b',
                    'aws_secret_access_key': 'b',
                    'filebase': 'some_prefix/'
                }
            }
        })
