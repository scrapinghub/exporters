import json
import shutil
import unittest
import boto
import datetime
import moto
import mock
from boto.exception import S3ResponseError
from boto.utils import compute_md5

from exporters.export_managers.s3_to_s3_bypass import S3Bypass, RequisitesNotMet
from exporters.exporter_config import ExporterConfig
from exporters.utils import remove_if_exists, TmpFile


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

    def test_items_limit_should_not_meet_conditions(self):
        # given:
        config = create_s3_bypass_simple_config()
        config.writer_options['options']['items_limit'] = 10

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
        with TmpFile() as tmp_filename:
            with open(tmp_filename, 'w') as f:
                f.write(json.dumps(self.data))
            with open(tmp_filename) as f:
                self.key_md5 = compute_md5(f)
        key.metadata = {'total': 2, 'md5': self.key_md5}
        key.set_contents_from_string(json.dumps(self.data))
        key.close()
        self.tmp_bypass_resume_file = 'tests/data/tmp_s3_bypass_resume_persistence.pickle'
        shutil.copyfile('tests/data/s3_bypass_resume_persistence.pickle', self.tmp_bypass_resume_file)

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
        self.assertEqual(bypass.total_items, 2, 'Bypass got an incorrect number of total items')

    @mock.patch('boto.s3.bucket.Bucket.copy_key', autospec=True)
    def test_copy_mode_bypass(self, copy_key_mock):
        copy_key_mock.side_effect = S3ResponseError(None, None)
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
        self.assertEqual(bypass.total_items, 2, 'Bypass got an incorrect number of total items')

    @mock.patch('boto.s3.connection.S3Connection.get_canonical_user_id', autospec=True)
    def test_copy_mode_bypass_when_cant_get_user_id(self, get_user_id_mock):
        get_user_id_mock.side_effect = S3ResponseError('Fake 403 Forbidden Error', None)
        # given
        bucket = self.s3_conn.create_bucket('dest_bucket')
        options = create_s3_bypass_simple_config()

        # when:
        bypass = S3Bypass(options)
        bypass.bypass()

        # then:
        key = next(iter(bucket.list('some_prefix/')))
        self.assertEqual(self.data, json.loads(key.get_contents_as_string()))

    def _create_and_populate_bucket(self, bucket_name, number_of_items=3):
        self.s3_conn.create_bucket(bucket_name)
        source_bucket = self.s3_conn.get_bucket(bucket_name)
        data = [
            {'name': 'Roberto', 'birthday': '12/05/1987'},
            {'name': 'Claudia', 'birthday': '21/12/1985'},
        ]
        for i in range(1, number_of_items+1):
            key = source_bucket.new_key('some_prefix/key{}'.format(i))
            key.metadata = {'total': 2}
            key.set_contents_from_string(json.dumps(data))

    def test_resume_bypass(self):
        # given
        options = create_s3_bypass_simple_config()
        options.reader_options['options']['bucket'] = 'resume_bucket'
        options.writer_options['options']['bucket'] = 'resume_dest_bucket'
        options.persistence_options['resume'] = True
        options.persistence_options['persistence_state_id'] = 'tmp_s3_bypass_resume_persistence.pickle'
        options.persistence_options['options']['file_path'] = 'tests/data/'
        # Initial state is:
        # copied = ['some_prefix/key1']
        # pending = ['some_prefix/key2', 'some_prefix/key3']
        self._create_and_populate_bucket('resume_bucket')

        self.s3_conn.create_bucket('resume_dest_bucket')
        dest_bucket = self.s3_conn.get_bucket('resume_bucket')
        key = dest_bucket.new_key('some_prefix/key1')
        key.set_contents_from_string('not overwritten')

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
        self.assertEquals(bypass.total_items, 6, 'Wrong number of items written')

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

    def test_write_pointer(self):
        # given:
        writer = {
            'name': 'exporters.writers.s3_writer.S3Writer',
            'options': {
                'bucket': 'pointer_fake_bucket',
                'aws_access_key_id': 'a',
                'aws_secret_access_key': 'a',
                'filebase': 'tests/',
                'save_pointer': 'pointer/LAST'
            }
        }

        options = create_s3_bypass_simple_config(writer=writer)
        self.s3_conn.create_bucket('pointer_fake_bucket')

        # when:
        bypass = S3Bypass(options)
        bypass.bypass()
        bypass.close()

        # then:
        bucket = self.s3_conn.get_bucket('pointer_fake_bucket')
        saved_keys = [k for k in bucket.list('pointer/')]
        self.assertEquals(1, len(saved_keys))
        key = saved_keys[0]
        self.assertEqual('tests/', key.get_contents_as_string())

    def test_prefix_pointer_list_keys(self):
        #given
        reader = {
            'name': 'exporters.readers.s3_reader.S3Reader',
            'options': {
                'bucket': 'source_pointer_bucket',
                'aws_access_key_id': 'a',
                'aws_secret_access_key': 'a',
                'prefix_pointer': 'test_pointer/LAST'
            }
        }

        writer = {
            'name': 'exporters.writers.s3_writer.S3Writer',
            'options': {
                'bucket': 'dest_pointer_bucket',
                'aws_access_key_id': 'b',
                'aws_secret_access_key': 'b',
                'filebase': 'some_prefix/'
            }
        }

        self._create_and_populate_bucket('source_pointer_bucket')
        bucket = self.s3_conn.get_bucket('source_pointer_bucket')

        expected_prefix_pointer = 'some_prefix/'
        key = bucket.new_key('test_pointer/LAST')
        key.set_contents_from_string(expected_prefix_pointer)
        key.close()

        self.s3_conn.create_bucket('dest_pointer_bucket')
        options = create_s3_bypass_simple_config(reader=reader, writer=writer)
        expected_keys = ['some_prefix/key1', 'some_prefix/key2', 'some_prefix/key3']

        #when
        bypass = S3Bypass(options)
        bypass.bypass()

        # then
        dest_bucket = self.s3_conn.get_bucket('dest_pointer_bucket')
        keys = dest_bucket.list(prefix='some_prefix/')
        keys_list = []
        for key in keys:
            keys_list.append(key.name)
        self.assertEqual(expected_keys, keys_list)

    def test_get_md5(self):
         # given
        self.s3_conn.create_bucket('dest_bucket')
        options = create_s3_bypass_simple_config()
        options.writer_options['options']['save_metadata'] = True

        # when:
        bypass = S3Bypass(options)
        bucket = self.s3_conn.get_bucket('source_bucket')
        key = bucket.get_key('some_prefix/test_key')

        with TmpFile() as tmp_filename:
            key.get_contents_to_filename(tmp_filename)
            metadata_md5 = bypass._get_md5(key, tmp_filename)

        # then:
        self.assertEqual(metadata_md5, self.key_md5)
