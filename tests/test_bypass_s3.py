import json
import shutil
import tempfile
import unittest
import boto
import datetime
import moto
import mock
from exporters.export_managers.s3_to_s3_bypass import S3Bypass, RequisitesNotMet
from exporters.exporter_config import ExporterConfig
from exporters.utils import remove_if_exists


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


def create_s3_bypass_simple_config(**kwargs):
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
        bypass = S3Bypass(create_s3_bypass_simple_config())
        # shouldn't raise any exception
        bypass.meets_conditions()

    def test_custom_filter_should_not_meet_conditions(self):
        # given:
        config = create_s3_bypass_simple_config(filter={
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
        config = create_s3_bypass_simple_config(grouper={
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
        self.tmp_bypass_resume_file = 'tests/data/tmp_s3_bypass_resume_persistence'
        shutil.copyfile('tests/data/s3_bypass_resume_persistence', self.tmp_bypass_resume_file)

    def tearDown(self):
        self.mock_s3.stop()
        remove_if_exists(self.tmp_bypass_resume_file)

    def test_copy_bypass_s3(self):
        # given
        self.s3_conn.create_bucket('dest_bucket')
        options = create_s3_bypass_simple_config()

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
        options = create_s3_bypass_simple_config()

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

    def _set_resume_prevstate(self, options):
        self.s3_conn.create_bucket('resume_dest_bucket')
        options.reader_options['options']['bucket'] = 'resume_bucket'
        options.writer_options['options']['bucket'] = 'resume_dest_bucket'
        options.persistence_options['resume'] = True
        options.persistence_options['persistence_state_id'] = 'tmp_s3_bypass_resume_persistence'
        options.persistence_options['options']['file_path'] = 'tests/data/'
        self.s3_conn.create_bucket('resume_bucket')
        source_bucket = self.s3_conn.get_bucket('resume_bucket')
        data = [
            {'name': 'Roberto', 'birthday': '12/05/1987'},
            {'name': 'Claudia', 'birthday': '21/12/1985'},
        ]
        for i in range(1, 4):
            key = source_bucket.new_key('some_prefix/key{}'.format(i))
            key.set_contents_from_string(json.dumps(data))
        
        dest_bucket = self.s3_conn.get_bucket('resume_bucket')
        key = dest_bucket.new_key('some_prefix/key1')
        key.set_contents_from_string('not overwritten')

    def test_resume_bypass(self):
        # given
        options = create_s3_bypass_simple_config()
        self._set_resume_prevstate(options)
        expected_final_keys = ['some_prefix/key1', 'some_prefix/key2', 'some_prefix/key3']

        # when:
        bypass = S3Bypass(options)
        bypass.bypass()

        # then:
        dest_bucket = self.s3_conn.get_bucket('resume_bucket')
        key1 = dest_bucket.get_key('some_prefix/key1')
        self.assertEqual(key1.get_contents_as_string(), 'not overwritten')
        bucket_keynames = [k.name for k in list(dest_bucket.list('some_prefix/'))]
        self.assertEquals(expected_final_keys, bucket_keynames)

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
        options = create_s3_bypass_simple_config(writer=writer)

        # when:
        bypass = S3Bypass(options)

        # then:
        filebase = bypass._get_filebase(options.writer_options['options'])
        self.assertEqual(expected, filebase)
