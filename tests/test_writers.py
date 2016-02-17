import gzip
import json
import os
import random
import unittest
import csv
from collections import Counter

from exporters.export_formatter.xml_export_formatter import XMLExportFormatter
from exporters.file_handlers import JsonFileHandler
from exporters.records.base_record import BaseRecord
from exporters.write_buffer import WriteBuffer
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
        self.writer.close()

    def test_write_not_implemented(self):
        with self.assertRaises(NotImplementedError):
            self.writer.write('', '')


class FakeWriter(BaseWriter):
    """CustomWriter writing records to self.custom_output
    to test BaseWriter extensibility
    """

    def __init__(self, *args, **kwargs):
        format = kwargs.pop('format', 'json')
        format_data = kwargs.pop('format_data', {})
        super(FakeWriter, self).__init__(*args, **kwargs)
        self.custom_output = {}
        self.fake_files_already_written = []
        format_info = BaseWriter.supported_file_extensions[format]
        self.write_buffer = WriteBuffer(1000, 1000, format_info, format_data)

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
            writer.flush()
        finally:
            writer.close()

        # then:
        output = writer.custom_output[()]
        self.assertEquals([json.dumps(item) for item in self.batch],
                          output.splitlines())
        self.assertEquals('jl', writer.write_buffer.items_group_files.file_extension)

    def test_write_buffer_removes_files(self):
        # given:
        self.batch = list(JsonExportFormatter({}).format(self.batch))
        writer = FakeWriter({})

        writer.write_buffer.items_per_buffer_write = 1

        # when:
        try:
            writer.write_batch(self.batch)
            # then
            self.assertGreater(len(writer.fake_files_already_written), 0)
            for f in writer.fake_files_already_written:
                self.assertFalse(os.path.exists(f))
                self.assertFalse(os.path.exists(f + '.gz'))
        finally:
            writer.close()

    def test_custom_writer_with_csv_formatter(self):
        # given:
        formatter = CSVExportFormatter(
                {'options': {'show_titles': False, 'fields': ['key1', 'key2']}})
        self.batch = list(formatter.format(self.batch))
        writer = FakeWriter({}, format='csv')

        # when:
        try:
            writer.write_batch(self.batch)
            writer.flush()
        finally:
            writer.close()

        # then:
        output = writer.custom_output[()].splitlines()
        self.assertEquals(
                [
                    ['value11', 'value21'],
                    ['value12', 'value22'],
                    ['value13', 'value23'],
                ],
                [l for l in csv.reader(output)])

        self.assertEquals('csv', writer.write_buffer.items_group_files.file_extension)

    def test_custom_writer_with_xml_formatter(self):
        from xml.dom.minidom import parseString
        # given:
        formatter = XMLExportFormatter({'options': {}})
        self.batch = list(formatter.format(self.batch))
        writer = FakeWriter({}, format='xml', format_data={'formatter': {'header': '<root>', 'bottom': '</root>'}})

        # when:
        try:
            writer.write_batch(self.batch)
            writer.flush()
        finally:
            writer.close()

        # then:
        output = writer.custom_output[()].splitlines()

        expected_list = [
            parseString(
                '<item><key2 type="str">value21</key2><key1 type="str">value11</key1></item>'),
            parseString(
                '<item><key2 type="str">value22</key2><key1 type="str">value12</key1></item>'),
            parseString(
                '<item><key2 type="str">value23</key2><key1 type="str">value13</key1></item>')
        ]
        expected = ['<root>'] + \
                   [{'key1': item.getElementsByTagName('key1')[0].firstChild.nodeValue,
                     'key2': item.getElementsByTagName('key2')[0].firstChild.nodeValue}
                    for item in expected_list] + \
                   ['</root>']

        out = [output[0]] + \
              [{'key1': parseString(l).getElementsByTagName('key1')[0].firstChild.nodeValue,
                'key2': parseString(l).getElementsByTagName('key2')[0].firstChild.nodeValue}
               for l in output[1:-1]] + \
              [output[-1]]

        self.assertEquals(expected, out)
        self.assertEquals('xml', writer.write_buffer.items_group_files.file_extension)

    def test_custom_writer_with_xml_formatter_with_options(self):
        from xml.dom.minidom import parseString
        # given:
        formatter = XMLExportFormatter(
                {'options': {'attr_type': False,
                             'fields_order': ['key1', 'key2'],
                             'item_name': 'XmlItem',
                             'root_name': 'RootItem'}})
        self.batch = list(formatter.format(self.batch))
        writer = FakeWriter({}, format='xml', format_data={'formatter': {'header': '<RootItem>', 'bottom': '</RootItem>'}})

        # when:
        try:
            writer.write_batch(self.batch)
            writer.flush()
        finally:
            writer.close()

        # then:
        output = writer.custom_output[()].splitlines()

        expected_list = [
            parseString(
                '<XmlItem><key1>value11</key1><key2>value21</key2></XmlItem>'),
            parseString(
                '<XmlItem><key1>value12</key1><key2>value22</key2></XmlItem>'),
            parseString(
                '<XmlItem><key1>value13</key1><key2>value23</key2></XmlItem>')
        ]
        expected = ['<RootItem>']
        expected += [[node.localName for node in item.getElementsByTagName('XmlItem')[0].childNodes]
                     for item in expected_list]
        expected += ['</RootItem>']
        out = [output[0]]
        out += [[node.localName for node in parseString(l).getElementsByTagName('XmlItem')[0].childNodes]
                for l in output[1:-1]]
        out += [output[-1]]

        self.assertEquals(expected, out)
        self.assertEquals('xml', writer.write_buffer.items_group_files.file_extension)

    def test_writer_stats(self):
        # given:
        self.batch = list(JsonExportFormatter({}).format(self.batch))
        writer = FakeWriter({})
        # when:
        try:
            writer.write_batch(self.batch)
            writer.flush()
        finally:
            writer.close()
        self.assertEqual([writer.items_count, writer.stats['written_items']], [3, 3])


class WriteBufferTest(unittest.TestCase):
    def setUp(self):
        format_info = {'format': 'jl', 'file_handler': JsonFileHandler}
        self.write_buffer = WriteBuffer(1000, 1000, format_info, {})

    def tearDown(self):
        self.write_buffer.close()

    def test_get_metadata(self):
        # given:
        self.write_buffer.metadata['somekey'] = {'items': 10}
        # then
        self.assertEqual(self.write_buffer.get_metadata('somekey', 'items'), 10,
                         'Wrong metadata')
        self.assertIsNone(self.write_buffer.get_metadata('somekey', 'nokey'))
        with self.assertRaises(KeyError):
            self.assertIsNone(self.write_buffer.get_metadata('nokey', 'nokey'))


class ConsoleWriterTest(unittest.TestCase):
    def setUp(self):
        self.options = {
            'log_level': 'DEBUG',
            'logger_name': 'export-pipeline'
        }
        self.writer = ConsoleWriter(self.options)

    def tearDown(self):
        self.writer.close()

    def test_write_console(self):
        items_to_write = []
        for i in range(0, 10):
            item = BaseRecord()
            item['key'] = i
            item['value'] = random.randint(0, 10000)
            items_to_write.append(item)

        self.writer.write_batch(items_to_write)
        self.assertEqual(self.writer.items_count, 10)


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
        writer.close()


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
        writer.close()
