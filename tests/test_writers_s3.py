import unittest

import boto
import moto
import mock

from exporters.meta import ExportMeta
from exporters.records.base_record import BaseRecord
from exporters.utils import TmpFile
from exporters.writers.base_writer import InconsistentWriteState
from exporters.writers.s3_writer import S3Writer

from .utils import meta

RESERVOIR_SAMPLING_BUFFER_CLASS = \
    'exporters.write_buffers.reservoir_sampling_buffer.ReservoirSamplingWriteBuffer'


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

    def tearDown(self):
        self.mock_s3.stop()

    def get_batch(self):
        data = [
            {'name': 'Roberto', 'birthday': '12/05/1987'},
            {'name': 'Claudia', 'birthday': '21/12/1985'},
        ]
        return [BaseRecord(d) for d in data]

    def test_write_s3(self):
        # given
        items_to_write = self.get_batch()
        options = self.get_writer_config()

        # when:
        writer = S3Writer(options, meta())
        try:
            writer.write_batch(items_to_write)
            writer.flush()
        finally:
            writer.close()

        # then:
        bucket = self.s3_conn.get_bucket('fake_bucket')
        saved_keys = [k for k in bucket.list()]
        self.assertEquals(1, len(saved_keys))
        self.assertEqual(saved_keys[0].name, 'tests/0.jl.gz')

    def test_write_s3_with_s3_prefix(self):
        # given
        items_to_write = self.get_batch()
        options = self.get_writer_config()
        options['options']['bucket'] = 's3://fake_bucket/'

        # when:
        writer = S3Writer(options, meta())
        try:
            writer.write_batch(items_to_write)
            writer.flush()
        finally:
            writer.close()

        # then:
        bucket = self.s3_conn.get_bucket('fake_bucket')
        saved_keys = [k for k in bucket.list()]
        self.assertEquals(1, len(saved_keys))
        self.assertEqual(saved_keys[0].name, 'tests/0.jl.gz')

    def test_write_s3_big_file(self):
        # given
        options = self.get_writer_config()

        # when:
        writer = S3Writer(options, meta())
        try:
            with TmpFile() as tmp_filename:
                with open(tmp_filename, 'w') as f:
                    f.truncate(1000)
                writer._upload_large_file(tmp_filename, 'tests/0.jl.gz')
        finally:
            writer.close()

        # then:
        bucket = self.s3_conn.get_bucket('fake_bucket')
        saved_keys = [k for k in bucket.list()]
        self.assertEquals(1, len(saved_keys))
        self.assertEqual(saved_keys[0].name, 'tests/0.jl.gz')

    def test_connect_to_specific_region(self):
        # given:
        conn = boto.connect_s3()
        conn.create_bucket('another_fake_bucket')

        options = self.get_writer_config()
        options['options']['aws_region'] = 'eu-west-1'
        options['options']['bucket'] = 'another_fake_bucket'

        # when:
        writer = S3Writer(options, meta())

        # then:
        self.assertEquals('eu-west-1', writer.aws_region)
        writer.close()

    def test_write_pointer(self):
        # given:
        conn = boto.connect_s3()
        conn.create_bucket('pointer_fake_bucket')

        options = self.get_writer_config()
        options['options']['save_pointer'] = 'pointer/LAST'
        options['options']['bucket'] = 'pointer_fake_bucket'

        items_to_write = self.get_batch()

        # when:
        try:
            writer = S3Writer(options, meta())
            writer.write_batch(items_to_write)
            writer.flush()
        finally:
            writer.close()

        # then:
        bucket = self.s3_conn.get_bucket('pointer_fake_bucket')
        saved_keys = [k for k in bucket.list('pointer/')]
        self.assertEquals(1, len(saved_keys))
        key = saved_keys[0]
        self.assertEqual('tests/', key.get_contents_as_string())

    @mock.patch('boto.s3.connection.S3Connection.get_bucket')
    def test_write_without_getting_validated_bucket(self, mock_get_bucket):
        import boto.s3.bucket

        def reject_validated_get_bucket(*args, **kwargs):
            if kwargs.get('validate', True):
                raise boto.exception.S3ResponseError("Fake Error", "Permission Denied")

            bucket = mock.Mock(spec=boto.s3.bucket.Bucket)
            bucket.name = 'bucket_name'
            return bucket

        mock_get_bucket.side_effect = reject_validated_get_bucket

        writer = S3Writer(self.get_writer_config(), meta())
        writer.close()

    def test_connect_to_bucket_location(self):
        # given:
        conn = boto.s3.connect_to_region('eu-west-1')
        conn.create_bucket('another_fake_bucket')

        options = self.get_writer_config()
        options['options']['bucket'] = 'another_fake_bucket'

        # when:
        writer = S3Writer(options, meta())

        # then:
        self.assertEquals('eu-west-1', writer.aws_region)
        writer.close()

    def get_writer_config(self):
        return {
            'name': 'exporters.writers.s3_writer.S3Writer',
            'options': {
                'bucket': 'fake_bucket',
                'aws_access_key_id': 'FAKE_ACCESS_KEY',
                'aws_secret_access_key': 'FAKE_SECRET_KEY',
                'filebase': 'tests/{file_number}',
            }
        }

    def test_write_s3_check_consistency_key_not_present(self):
        # given
        items_to_write = self.get_batch()
        options = self.get_writer_config()
        options['options']['check_consistency'] = True

        # when:
        try:
            writer = S3Writer(options, ExportMeta(options))
            writer.write_batch(items_to_write)
            writer.flush()
        finally:
            writer.close()
        bucket = self.s3_conn.get_bucket('fake_bucket')
        bucket.delete_key('tests/0.jl.gz')

        # then:
        with self.assertRaisesRegexp(InconsistentWriteState, 'not found in bucket'):
            writer.finish_writing()

    def test_write_s3_check_consistency_wrong_size(self):
        # given
        items_to_write = self.get_batch()
        options = self.get_writer_config()
        options['options']['check_consistency'] = True

        # when:
        try:
            writer = S3Writer(options, ExportMeta(options))
            writer.write_batch(items_to_write)
            writer.flush()
        finally:
            writer.close()
        bucket = self.s3_conn.get_bucket('fake_bucket')
        key = bucket.get_key('tests/0.jl.gz')
        key.set_contents_from_string('fake contents')

        # then:
        with self.assertRaisesRegexp(InconsistentWriteState, 'has unexpected size'):
            writer.finish_writing()

    def test_write_s3_check_consistency_wrong_items_count(self):
        # given
        items_to_write = self.get_batch()
        options = self.get_writer_config()
        options['options']['check_consistency'] = True

        # when:
        try:
            writer = S3Writer(options, ExportMeta(options))
            writer.write_batch(items_to_write)
            writer.flush()
        finally:
            writer.close()
        bucket = self.s3_conn.get_bucket('fake_bucket')
        key = bucket.get_key('tests/0.jl.gz')
        content = key.get_contents_as_string()
        bucket.delete_key('tests/0.jl.gz')
        new_key = bucket.new_key('tests/0.jl.gz')
        new_key.update_metadata({'total': 999})
        new_key.set_contents_from_string(content)

        # then:
        with self.assertRaisesRegexp(InconsistentWriteState, 'Unexpected number of records'):
            writer.finish_writing()

    def test_write_reservoir_sample_s3(self):
        # given
        sample_size = 10
        items_to_write = [BaseRecord({u'key1': u'value1{}'.format(i),
                                     u'key2': u'value2{}'.format(i)}) for i in range(100)]
        options = self.get_writer_config()
        options['options'].update({
                                  'compression': 'none',
                                  'write_buffer': RESERVOIR_SAMPLING_BUFFER_CLASS,
                                  'write_buffer_options': {'sample_size': sample_size}})

        # when:
        writer = S3Writer(options, meta())
        try:
            writer.write_batch(items_to_write)
            writer.flush()
        finally:
            writer.close()

        # then:
        bucket = self.s3_conn.get_bucket('fake_bucket')
        saved_keys = [k for k in bucket.list()]
        self.assertEquals(1, len(saved_keys))
        self.assertEqual(saved_keys[0].name, 'tests/0.jl')
        content = saved_keys[0].get_contents_as_string()
        self.assertEquals(len(content.strip().splitlines()), sample_size)
        self.assertNotEquals(content.strip().splitlines(), items_to_write[:sample_size])
