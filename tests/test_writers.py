import gzip
import json
import os
import random
import unittest
import csv

from exporters.records.base_record import BaseRecord
from exporters.writers import FSWriter
from exporters.writers.base_writer import BaseWriter
from exporters.writers.console_writer import ConsoleWriter
from exporters.writers.filebase_base_writer import FilebaseBaseWriter
from exporters.export_formatter.csv_export_formatter import CSVExportFormatter
from exporters.export_formatter.json_export_formatter import JsonExportFormatter


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


class FakeWriter(BaseWriter):
    """CustomWriter writing records to self.custom_output
    to test BaseWriter extensibility
    """
    def __init__(self, *args, **kwargs):
        super(FakeWriter, self).__init__(*args, **kwargs)
        self.custom_output = {}
        self.fake_files_already_written = []

    def write(self, path, key):
        with gzip.open(path) as f:
            self.custom_output[key] = f.read()
        self.fake_files_already_written.append(path)


class CustomWriterTest(unittest.TestCase):
    def setUp(self):
        self.batch = [
            BaseRecord({u'key1': u'value11', u'key2': u'value21'}),
            BaseRecord({u'key1': u'value12', u'key2': u'value22'}),
            BaseRecord({u'key1': u'value13', u'key2': u'value23'}),
        ]

    def test_custom_writer(self):
        # given:
        self.batch = list(JsonExportFormatter({}).format(self.batch))
        writer = FakeWriter({})

        # when:
        try:
            writer.write_batch(self.batch)
        finally:
            writer.close_writer()

        # then:
        output = writer.custom_output[()]
        self.assertEquals([json.dumps(item) for item in self.batch],
                          output.splitlines())
        self.assertEquals('jl', writer.file_extension)

    def test_write_buffer_removes_files(self):
        # given:
        self.batch = list(JsonExportFormatter({}).format(self.batch))
        writer = FakeWriter({})
        writer.items_per_buffer_write = 1

        # when:
        try:
            writer.write_batch(self.batch)
            # then
            self.assertGreater(len(writer.fake_files_already_written), 0)
            for f in writer.fake_files_already_written:
                self.assertFalse(os.path.exists(f))
                self.assertFalse(os.path.exists(f+'.gz'))
        finally:
            writer.close_writer()

    def test_custom_writer_with_csv_formatter(self):
        # given:
        formatter = CSVExportFormatter({'options': {'show_titles': False, 'fields': ['key1', 'key2']}})
        self.batch = list(formatter.format(self.batch))
        writer = FakeWriter({})

        # when:
        try:
            writer.write_batch(self.batch)
        finally:
            writer.close_writer()

        # then:
        output = writer.custom_output[()].splitlines()
        self.assertEquals(
            [
                ['value11', 'value21'],
                ['value12', 'value22'],
                ['value13', 'value23'],
            ],
            [l for l in csv.reader(output)])
        self.assertEquals('csv', writer.file_extension)

    def test_writer_stats(self):
        # given:
        self.batch = list(JsonExportFormatter({}).format(self.batch))
        writer = FakeWriter({})
        # when:
        try:
            writer.write_batch(self.batch)
        finally:
            writer.close_writer()
        self.assertEqual(writer.stats['items_count'], 3)
        for key in writer.stats['written_keys']['keys']:
            self.assertEqual(writer.stats['written_keys']['keys'][key]['number_of_records'], 3)


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
        self.assertEqual(self.writer.stats['items_count'], 10)


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
