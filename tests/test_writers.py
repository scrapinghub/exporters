import os
import random
import shutil
import tempfile
import unittest
from exporters.records.base_record import BaseRecord
from exporters.writers.base_writer import BaseWriter
from exporters.writers.console_writer import ConsoleWriter
from exporters.writers.odo_writer import ODOWriter


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
