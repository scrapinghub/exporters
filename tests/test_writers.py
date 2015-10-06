import os
import random
import shutil
import tempfile
import unittest
from exporters.records.base_record import BaseRecord
from exporters.writers import FSWriter
from exporters.writers.base_writer import BaseWriter
from exporters.writers.console_writer import ConsoleWriter
from exporters.writers.odo_writer import ODOWriter
from exporters.writers.filebase_base_writer import FilebaseBaseWriter


class BaseWriterTest(unittest.TestCase):
    def setUp(self):
        self.options = {
            'log_level': 'DEBUG',
            'logger_name': 'export-pipeline'
        }
        self.writer = BaseWriter(self.options)

    def tearDown(self):
        self.writer.close_writer()

    def test_write_not_implemented(self):
        with self.assertRaises(NotImplementedError):
            self.writer.write('', '')


class ConsoleWriterTest(unittest.TestCase):
    def setUp(self):
        self.options = {
            'log_level': 'DEBUG',
            'logger_name': 'export-pipeline'
        }
        self.writer = ConsoleWriter(self.options)

    def tearDown(self):
        self.writer.close_writer()

    def test_write_console(self):
        items_to_write = []
        for i in range(0, 10):
            item = BaseRecord()
            item['key'] = i
            item['value'] = random.randint(0, 10000)
            items_to_write.append(item)

        self.writer.write_batch(items_to_write)
        self.assertEqual(self.writer.items_count, 10)


class OdoWriterTest(unittest.TestCase):

    def setUp(self):
        self.batch_path = 'tests/data/test_data.jl.gz'

        self.tmp_path = tempfile.mkdtemp()
        self.tmp_file = os.path.join(self.tmp_path, 'test.csv')

        self.schema = {'$schema': u'http://json-schema.org/draft-04/schema',
                       'required': [u'item'], 'type': 'object',
                       'properties': {u'item': {'type': 'string'}}}
        self.writer_config = {
            'options': {
                'odo_uri': self.tmp_file,
                'schema': self.schema
            }
        }

    def test_write_csv(self):
        writer = ODOWriter(self.writer_config)
        writer.write(self.batch_path, [])
        writer.close_writer()
        with open(self.tmp_file) as f:
            lines = f.readlines()
        self.assertEqual(lines, ['item\n', 'value1\n', 'value2\n', 'value3\n'])
        shutil.rmtree(self.tmp_path)


class FilebaseBaseWriterTest(unittest.TestCase):

    def test_get_file_number_not_implemented(self):
        writer_config = {
            'options': {
                'filebase': '/tmp/'
            }
        }
        writer = FilebaseBaseWriter(writer_config)
        self.assertIsInstance(writer.get_file_suffix('', ''), basestring)
        path, file_name = writer.create_filebase_name([])
        self.assertEqual(path, '/tmp')
        writer.close_writer()


class FSWriterTest(unittest.TestCase):

    def test_get_file_number(self):
        writer_config = {
            'options': {
                'filebase': '/tmp/exporter_test'
            }
        }
        writer = FSWriter(writer_config)
        self.assertEqual(writer.get_file_suffix('test', 'test'), '0000')
        path, file_name = writer.create_filebase_name([])
        self.assertEqual(path, '/tmp')
        self.assertEqual(file_name, 'exporter_test0000.gz')
        writer.close_writer()
