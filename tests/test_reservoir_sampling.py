import gzip
import json
import os
import shutil
import tempfile
import unittest
from contextlib import closing
from exporters.records.base_record import BaseRecord
from exporters.writers import FSWriter
from exporters.writers.base_writer import BaseWriter
from exporters.groupers import PythonExpGrouper
from exporters.writers.filebase_base_writer import FilebaseBaseWriter
from .utils import meta


class FakeWriter(BaseWriter):
    """CustomWriter writing records to self.custom_output
    to test BaseWriter extensibility
    """

    def __init__(self, options, *args, **kwargs):
        super(FakeWriter, self).__init__(options, meta(), *args, **kwargs)
        self.custom_output = {}
        self.fake_files_already_written = []
        self.set_metadata('written_files', self.fake_files_already_written)

    def write(self, path, key):
        with gzip.open(path) as f:
            self.custom_output[key] = f.read()
        self.fake_files_already_written.append(path)


class FakeFilebaseWriter(FilebaseBaseWriter):
    """CustomWriter writing records to self.custom_output
    to test BaseWriter extensibility
    """

    def __init__(self, options, *args, **kwargs):
        super(FakeFilebaseWriter, self).__init__(options, meta(), *args, **kwargs)
        self.custom_output = {}
        self.fake_files_already_written = []
        self.set_metadata('written_files', self.fake_files_already_written)

    def write(self, path, key, file_name=None):
        if file_name:
            with open(path) as f:
                self.custom_output[key] = f.read()
            self.fake_files_already_written.append(file_name)
        else:
            with gzip.open(path) as f:
                self.custom_output[key] = f.read()
            self.fake_files_already_written.append(path)


class CustomWriterTest(unittest.TestCase):
    def setUp(self):
        self.sample_size = 10
        self.batch = [BaseRecord({u'key1': u'value1{}'.format(i),
                                 u'key2': u'value2{}'.format(i)}) for i in range(100)]

    def run_fake_writer(self):
        # given:
        writer = FakeWriter({'options': {'reservoir_sampling': True,
                            'items_per_buffer_write': self.sample_size}},
                            {})
        # when:
        try:
            writer.write_batch(self.batch)
            writer.flush()
        finally:
            writer.close()

        # then:
        return writer.custom_output[()]

    def test_sample_writer(self):
        output = self.run_fake_writer()
        self.assertEquals(self.sample_size, len(output.splitlines()))
        # test duplicates
        self.assertEquals(self.sample_size, len(set(output.splitlines())))

    def test_different_samples(self):
        outputs = [self.run_fake_writer() for i in range(2)]
        self.assertNotEquals(outputs[0].splitlines(), outputs[1].splitlines())


class FilebaseBaseWriterTest(unittest.TestCase):

    def test_get_file_number_not_implemented(self):
        writer_config = {
            'options': {
                'filebase': '/tmp/',
                'reservoir_sampling': True,
                'items_per_buffer_write': 10
            }
        }
        writer = FilebaseBaseWriter(writer_config, meta())
        self.assertIsInstance(writer.get_file_suffix('', ''), basestring)
        path, file_name = writer.create_filebase_name([])
        self.assertEqual(path, '/tmp')
        writer.close()


class FSWriterTest(unittest.TestCase):

    def get_batch(self):
        data = [
            {'name': 'Roberto', 'birthday': '12/05/1987'},
            {'name': 'Claudia', 'birthday': '21/12/1985'},
        ]
        return [BaseRecord(d) for d in data]

    def get_writer_config(self):
        return {
            'name': 'exporters.writers.fs_writer.FSWriter',
            'options': {
                'filebase': '{}/exporter_test'.format(self.tmp_dir),
                'reservoir_sampling': True,
                'items_per_buffer_write': self.sample_size
            }
        }

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.sample_size = 10

    def tearDown(self):
        try:
            shutil.rmtree(self.tmp_dir)
        except OSError:
            pass

    def test_get_file_number(self):
        writer_config = self.get_writer_config()
        writer = FSWriter(writer_config, meta())
        try:
            writer.write_batch(self.get_batch())
            writer.flush()

        finally:
            writer.close()
        expected_file = '{}/exporter_test0000.jl.gz'.format(self.tmp_dir)
        self.assertTrue(expected_file in writer.written_files)

    def test_compression_gzip_format(self):
        writer_config = self.get_writer_config()
        writer_config['options'].update({'compression': 'gz'})
        writer = FSWriter(writer_config, meta())
        try:
            writer.write_batch(self.get_batch())
            writer.flush()

        finally:
            writer.close()
        expected_file = '{}/exporter_test0000.jl.gz'.format(self.tmp_dir)
        self.assertTrue(expected_file in writer.written_files)

        written = []
        with gzip.open(expected_file, 'r') as fin:
            for line in fin:
                written.append(json.loads(line))
        self.assertEqual(written, self.get_batch())

    def test_sample(self):
        batch = [BaseRecord({u'key1': u'value1{}'.format(i),
                            u'key2': u'value2{}'.format(i)}) for i in range(100)]

        writer_config = self.get_writer_config()
        writer = FSWriter(writer_config, meta())
        try:
            writer.write_batch(batch)
            writer.flush()

        finally:
            writer.close()

        written = []
        with gzip.open('{}/exporter_test0000.jl.gz'.format(self.tmp_dir), 'r') as fin:
            for line in fin:
                written.append(json.loads(line))
        self.assertEqual(len(written), self.sample_size)

    def _build_grouped_batch(self, batch, python_expressions):
        grouper_options = {
            'name': 'exporters.groupers.python_exp_grouper.PythonExpGrouper',
            'options': {'python_expressions': python_expressions}
        }
        grouper = PythonExpGrouper(options=grouper_options)
        return grouper.group_batch(batch)

    def test_writer_with_grouped_data(self):
        # given:
        batch = [
            BaseRecord(city=u'Madrid', country=u'ES',
                       monument='{}'.format(i)) for i in range(10)
        ]
        batch.extend(BaseRecord(city=u'Valencia', country=u'ES',
                     monument='{}'.format(i)) for i in range(10))
        batch.extend(BaseRecord(city=u'Paris', country=u'FR',
                     monument='{}'.format(i)) for i in range(10))

        grouped_batch = self._build_grouped_batch(
            batch, python_expressions=["item['country']", "item['city']"])

        options = self.get_writer_config()
        options['options']['filebase'] = os.path.join(self.tmp_dir, '{groups[0]}/{groups[1]}/file')
        options['options']['items_per_buffer_write'] = 2
        writer = FSWriter(options=options, metadata=meta())

        # when:
        with closing(writer) as w:
            w.write_batch(grouped_batch)
            w.flush()
            w.finish_writing()

        # then:
        expected_files = [
            'ES/Madrid/file0000.jl.gz',
            'ES/Valencia/file0000.jl.gz',
            'FR/Paris/file0000.jl.gz',
        ]
        expected = [os.path.join(self.tmp_dir, f) for f in expected_files]

        def listdir_recursive(path):
            return [os.path.join(d, f)
                    for d, _, fnames in os.walk(path)
                    for f in fnames]

        self.assertEqual(sorted(expected), sorted(listdir_recursive(self.tmp_dir)))
