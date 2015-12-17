import json
import tempfile
import unittest
import boto
import datetime
import moto
import mock
from exporters.export_managers.s3_to_s3_bypass import S3Bypass, RequisitesNotMet, \
    S3BypassResume
from exporters.exporter_config import ExporterConfig
from exporters.persistence.base_persistence import BasePersistence


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


def get_config(**kwargs):
    config = {
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
    }
    config.update(kwargs)
    return ExporterConfig(config)


class S3BypassConditionsTest(unittest.TestCase):
    def test_should_meet_conditions(self):
        bypass = S3Bypass(get_config())
        # shouldn't raise any exception
        bypass.meets_conditions()

    def test_custom_filter_should_not_meet_conditions(self):
        # given:
        config = get_config(filter={
            'name': 'exporters.filters.PythonexpFilter',
            'options': {'python_expression': 'None'}
        })

        # when:
        bypass = S3Bypass(config)

        # then:
        with self.assertRaises(RequisitesNotMet):
            bypass.meets_conditions()

    def test_custom_grouper_should_not_meet_conditions(self):
        # given:
        config = get_config(grouper={
            'name': 'whatever.Grouper',
        })

        # when:
        bypass = S3Bypass(config)

        # then:
        with self.assertRaises(RequisitesNotMet):
            bypass.meets_conditions()


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
        options = get_config()

        # when:
        bypass = S3Bypass(options)
        bypass.bypass()

        # then:
        bucket = self.s3_conn.get_bucket('dest_bucket')
        key = next(iter(bucket.list('some_prefix/')))
        self.assertEquals('some_prefix/test_key', key.name)
        self.assertEqual(self.data, json.loads(key.get_contents_as_string()))

    def test_copy_mode_bypass(self):
        # given
        self.s3_conn.create_bucket('dest_bucket')
        options = get_config()

        # when:
        bypass = S3Bypass(options)
        bypass.copy_mode = False
        bypass.tmp_folder = tempfile.mkdtemp()
        bypass.bypass()

        # then:
        bucket = self.s3_conn.get_bucket('dest_bucket')
        key = next(iter(bucket.list('some_prefix/')))
        self.assertEquals('some_prefix/test_key', key.name)
        self.assertEqual(self.data, json.loads(key.get_contents_as_string()))

    def test_filebase_format_bypass(self):
        # given

        writer = {
              'name': 'exporters.writers.s3_writer.S3Writer',
              'options': {
                'bucket': 'a',
                'aws_access_key_id': 'a',
                'aws_secret_access_key': 'a',
                'filebase': 'some_path/%Y-%m-%d/'
              }
        }

        expected = 'some_path/%Y-%m-%d/'.format(datetime.datetime.now())
        expected = datetime.datetime.now().strftime(expected)
        options = get_config(writer=writer)

        # when:
        bypass = S3Bypass(options)

        # then:
        filebase = bypass._get_filebase(options.writer_options['options'])
        self.assertEqual(expected, filebase)


class FakePersistence(BasePersistence):

    def get_last_position(self):
        return {'pending': ['key2', 'key3', 'key4'], 'done': ['key1']}

    def generate_new_job(self):
        pass

    def close(self):
        pass


class FakeS3BypassResume(S3BypassResume):

    def __init__(self, config):
        self.config = config
        self.state = FakePersistence(config.persistence_options)
        self.position = self.state.get_last_position()
        self._retrieve_keys()


class S3BypassResumeTest(unittest.TestCase):

    def test_bypass_resume(self):
        # given
        expected_keys = ['key2', 'key3', 'key4']
        config = get_config()

        # when:
        bypass_resume = FakeS3BypassResume(config)

        # then:
        self.assertEqual(bypass_resume.keys, expected_keys)
