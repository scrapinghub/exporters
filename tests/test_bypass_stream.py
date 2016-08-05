import shutil
import unittest
from contextlib import closing

import mock
from six import BytesIO
from exporters.bypasses.stream_bypass import StreamBypass, Stream
from exporters.exporter_config import ExporterConfig
from exporters.utils import remove_if_exists
from exporters.iterio import IterIO
from .utils import meta


def create_stream_bypass_simple_config(**kwargs):
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
            'name': 'exporters.writers.gstorage_writer.GStorageWriter',
            'options': {
                'bucket': 'dest_bucket',
                'project': "",
                'credentials': {},
                'filebase': 'some_prefix/',
            }
        }
    }
    config.update(kwargs)
    return ExporterConfig(config)


class FooFile(object):
    """ file-like object that returns f's """
    def read(self, num_bytes):
        return "f"*num_bytes


class StreamBypassConditionsTest(unittest.TestCase):
    def test_should_meet_conditions(self):
        config = create_stream_bypass_simple_config()
        # shouldn't raise any exception
        StreamBypass.meets_conditions(config)

    def test_custom_filter_should_not_meet_conditions(self):
        # given:
        config = create_stream_bypass_simple_config(filter={
            'name': 'exporters.filters.PythonexpFilter',
            'options': {'python_expression': 'None'}
        })

        # when:

        # then:
        self.assertFalse(StreamBypass.meets_conditions(config))

    def test_custom_grouper_should_not_meet_conditions(self):
        # given:
        config = create_stream_bypass_simple_config(grouper={
            'name': 'whatever.Grouper',
        })

        # when:

        # then:
        self.assertFalse(StreamBypass.meets_conditions(config))

    def test_items_limit_should_not_meet_conditions(self):
        # given:
        config = create_stream_bypass_simple_config()
        config.writer_options['options']['items_limit'] = 10

        # when:

        # then:
        self.assertFalse(StreamBypass.meets_conditions(config))


class StreamBypassTest(unittest.TestCase):
    bypass_resume_file = 'stream_bypass_resume_persistence.pickle'
    tmp_bypass_resume_file = 'tmp_' + bypass_resume_file
    data_dir = 'tests/data/'

    def setUp(self):
        shutil.copyfile(self.data_dir + self.bypass_resume_file,
                        self.data_dir + self.tmp_bypass_resume_file)

    def tearDown(self):
        remove_if_exists(self.data_dir + self.tmp_bypass_resume_file)

    @mock.patch('gcloud.storage.Client')
    @mock.patch('boto.connect_s3')
    @mock.patch('exporters.readers.s3_reader.S3Reader.get_read_streams')
    @mock.patch('exporters.readers.s3_reader.S3Reader.open_stream')
    @mock.patch('exporters.writers.gstorage_writer.GStorageWriter.write_stream')
    def test_bypass_stream(self, write_stream_mock, open_stream_mock,
                           get_read_streams_mock, *othermocks):
        # given
        file_len = 50
        file_obj = IterIO(BytesIO('a'*file_len))
        get_read_streams_mock.return_value = [Stream('name', file_len, None)]
        open_stream_mock.return_value = file_obj
        options = create_stream_bypass_simple_config()

        # when:
        with closing(StreamBypass(options, meta())) as bypass:
            bypass.execute()

        # then:
        write_stream_mock.assert_called_once_with(Stream('name', file_len, None), file_obj)
        self.assertEquals(bypass.bypass_state.stats['bytes_copied'], 50,
                          'Wrong number of bytes written')

    @mock.patch('gcloud.storage.Client')
    @mock.patch('boto.connect_s3')
    @mock.patch('exporters.readers.s3_reader.S3Reader.get_read_streams')
    @mock.patch('exporters.readers.s3_reader.S3Reader.open_stream')
    @mock.patch('exporters.writers.gstorage_writer.GStorageWriter.write_stream')
    def test_resume_bypass(self, write_stream_mock, open_stream_mock,
                           get_streams_mock, *othermocks):
        # given
        options = create_stream_bypass_simple_config()
        options.persistence_options.update(
            resume=True,
            persistence_state_id=self.tmp_bypass_resume_file
        )
        options.persistence_options['options']['file_path'] = self.data_dir
        file_len = 50
        file_obj_b = IterIO(BytesIO('b'*file_len))
        stream_a = Stream('file_a', file_len, None)
        stream_b = Stream('file_b', file_len, None)
        get_streams_mock.return_value = [stream_a, stream_b]
        open_stream_mock.return_value = file_obj_b
        # Initial state is:
        # done = [(file_a, 50, None)] stats = {'bytes_copied': 50}

        # when:
        with closing(StreamBypass(options, meta())) as bypass:
            bypass.execute()

        # then:
        write_stream_mock.assert_called_once_with(stream_b, file_obj_b)
        assert bypass.bypass_state.stats['bytes_copied'] == 100,\
            'Wrong number of bytes written'
