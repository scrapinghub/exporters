import bz2
import csv
import datetime
import gzip
import json
import os
import random
import shutil
import tempfile
import unittest
from contextlib import closing

import mock

from exporters.exceptions import ConfigurationError
from exporters.export_formatter.csv_export_formatter import CSVExportFormatter
from exporters.export_formatter.json_export_formatter import JsonExportFormatter
from exporters.export_formatter.xml_export_formatter import XMLExportFormatter
from exporters.groupers import PythonExpGrouper
from exporters.records.base_record import BaseRecord
from exporters.write_buffer import WriteBuffer, ItemsGroupFilesHandler
from exporters.writers import FSWriter
from exporters.writers.base_writer import BaseWriter, InconsistentWriteState
from exporters.writers.console_writer import ConsoleWriter
from exporters.writers.filebase_base_writer import FilebaseBaseWriter
from .utils import meta


class BaseWriterTest(unittest.TestCase):
    def setUp(self):
        self.options = {
            'log_level': 'DEBUG',
            'logger_name': 'export-pipeline'
        }
        self.writer = BaseWriter(
            self.options, meta(), export_formatter=JsonExportFormatter(dict()))

    def tearDown(self):
        self.writer.close()

    def test_write_not_implemented(self):
        with self.assertRaises(NotImplementedError):
            self.writer.write('', '')


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
        self.batch = [
            BaseRecord({u'key1': u'value11', u'key2': u'value21'}),
            BaseRecord({u'key1': u'value12', u'key2': u'value22'}),
            BaseRecord({u'key1': u'value13', u'key2': u'value23'}),
        ]

    def test_custom_writer(self):
        # given:
        writer = FakeWriter({}, {}, export_formatter=JsonExportFormatter(dict()))

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
        writer = FakeWriter({}, {}, export_formatter=JsonExportFormatter(dict()))
        writer.write_buffer.items_per_buffer_write = 1

        # when:
        try:
            writer.write_batch(self.batch)
            # then
            self.assertEqual(len(writer.fake_files_already_written), 3,
                             'Wrong number of files written')
            for f in writer.fake_files_already_written:
                self.assertFalse(os.path.exists(f))
                self.assertFalse(os.path.exists(f[:-3]))
        finally:
            writer.close()

    def test_custom_writer_with_csv_formatter(self):
        # given:

        options = {
            'name': 'exporters.export_formatter.csv_export_formatter.CSVExportFormatter',
            'options': {'show_titles': False, 'fields': ['key1', 'key2']}
        }
        formatter = CSVExportFormatter(options)
        writer = FakeWriter({}, {}, export_formatter=formatter)

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
            [l for l in csv.reader(output)]
        )

        self.assertEquals('csv', writer.write_buffer.items_group_files.file_extension)

    def test_custom_writer_with_xml_formatter(self):
        from xml.dom.minidom import parseString
        # given:
        options = {
            'name': 'exporters.export_formatter.xml_export_formatter.XMLExportFormatter',
            'options': {

            }
        }
        formatter = XMLExportFormatter(options)
        writer = FakeWriter({}, {}, export_formatter=formatter)

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
        expected = ['<?xml version="1.0" encoding="UTF-8"?>', '<root>'] + \
                   [{'key1': item.getElementsByTagName('key1')[0].firstChild.nodeValue,
                     'key2': item.getElementsByTagName('key2')[0].firstChild.nodeValue}
                    for item in expected_list] + \
                   ['</root>']

        out = [output[0], output[1]] + \
              [{'key1': parseString(l).getElementsByTagName('key1')[0].firstChild.nodeValue,
                'key2': parseString(l).getElementsByTagName('key2')[0].firstChild.nodeValue}
               for l in output[2:-1]] + \
              [output[-1]]

        self.assertEquals(expected, out)
        self.assertEquals('xml', writer.write_buffer.items_group_files.file_extension)

    def test_custom_writer_with_xml_formatter_with_options(self):
        from xml.dom.minidom import parseString
        # given:
        options = {'name': 'exporters.export_formatter.xml_export_formatter.XMLExportFormatter',
                   'options': {
                       'attr_type': False,
                       'fields_order': ['key1', 'key2'],
                       'item_name': 'XmlItem',
                       'root_name': 'RootItem'}
                   }
        formatter = XMLExportFormatter(options)
        writer = FakeWriter({}, {}, export_formatter=formatter)

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
        expected = ['<?xml version="1.0" encoding="UTF-8"?>', '<RootItem>']
        expected += [[node.localName for node in item.getElementsByTagName('XmlItem')[0].childNodes]
                     for item in expected_list]
        expected += ['</RootItem>']
        out = [output[0], output[1]]
        out += [
            [
                node.localName
                for node in parseString(l).getElementsByTagName('XmlItem')[0].childNodes
            ]
            for l in output[2:-1]
        ]
        out += [output[-1]]

        self.assertEquals(expected, out)
        self.assertEquals('xml', writer.write_buffer.items_group_files.file_extension)

    def test_md5sum_file(self):
        # given:
        formatter = JsonExportFormatter({})
        with tempfile.NamedTemporaryFile() as tmp:
            writer = FakeFilebaseWriter(
                {'options': {'filebase': tmp.name, 'generate_md5': True}},  {},
                export_formatter=formatter)
            # when:
            try:
                writer.write_batch(self.batch)
                writer.flush()
                writer.finish_writing()
            finally:
                writer.close()
            self.assertIn('md5checksum.md5', writer.fake_files_already_written)

    @mock.patch('exporters.writers.base_writer.BaseWriter._check_write_consistency')
    def test_consistency_check(self, consistency_mock):
        # given:
        writer = FakeWriter({'options': {'check_consistency': True}},
                            export_formatter=JsonExportFormatter(dict()))

        # when:
        try:
            writer.write_batch(self.batch)
            writer.flush()
            writer.finish_writing()
        finally:
            writer.close()

        # then:
        consistency_mock.assert_called_once_with()

    def test_custom_writer_with_json_file_formatter(self):
        # given:
        options = {
            'name': 'exporters.export_formatter.json_export_formatter.JSONExportFormatter',
            'options': {
                'jsonlines': False
            }
        }
        formatter = JsonExportFormatter(options)
        writer = FakeWriter({}, {}, export_formatter=formatter)

        # when:
        try:
            writer.write_batch(self.batch)
            writer.flush()
        finally:
            writer.close()

        # then:
        output = writer.custom_output[()]
        out = json.loads(output)

        self.assertEquals(self.batch, out)
        self.assertEquals('json', writer.write_buffer.items_group_files.file_extension)


class WriteBufferTest(unittest.TestCase):
    def setUp(self):
        item_writer = ItemsGroupFilesHandler(JsonExportFormatter({}))
        self.write_buffer = WriteBuffer(1000, 1000, item_writer)

    def tearDown(self):
        self.write_buffer.close()

    def test_get_metadata(self):
        # given:
        self.write_buffer.metadata['somekey'] = {'items': 10}
        # then
        self.assertEqual(self.write_buffer.get_metadata('somekey', 'items'), 10,
                         'Wrong metadata')
        self.assertIsNone(self.write_buffer.get_metadata('somekey', 'nokey'))


class ConsoleWriterTest(unittest.TestCase):
    def setUp(self):
        self.options = {
            'log_level': 'DEBUG',
            'logger_name': 'export-pipeline'
        }
        self.writer = ConsoleWriter(
            self.options, meta(),
            export_formatter=JsonExportFormatter(dict()))

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
        self.assertEqual(self.writer.get_metadata('items_count'), 10)


class FilebaseBaseWriterTest(unittest.TestCase):

    def test_get_file_number_not_implemented(self):
        writer_config = {
            'options': {
                'filebase': '/tmp/',
            }
        }
        writer = FilebaseBaseWriter(writer_config, meta(),
                                    export_formatter=JsonExportFormatter(dict()))
        self.assertIsInstance(writer.get_file_suffix('', ''), basestring)
        path, file_name = writer.create_filebase_name([])
        self.assertEqual(path, '/tmp')
        writer.close()

    def test_get_full_filebase(self):
        writer_config = {
            'options': {
                'filebase': '/tmp/some_file_',
            }
        }
        writer = FilebaseBaseWriter(writer_config, meta(),
                                    export_formatter=JsonExportFormatter(dict()))
        writer.close()
        self.assertEqual(writer.filebase, '/tmp/some_file_')

    def test_create_filebase_name(self):
        writer_config = {
            'options': {
                'filebase': '/tmp/%m/%Y-some_folder_{groups[0]}/{groups[1]}_{file_number}_',
            }
        }
        writer = FilebaseBaseWriter(writer_config, meta(),
                                    export_formatter=JsonExportFormatter(dict()))
        writer.close()
        date = datetime.datetime.now()
        expected = (date.strftime('/tmp/%m/%Y-some_folder_g1'), 'filename')
        self.assertEqual(writer.create_filebase_name(('g1', 'g2'), file_name='filename'), expected)

    def test_wrong_file_number_in_filebase(self):
        writer_config = {
            'options': {
                'filebase': '/tmp/%m/%Y-some_folder_{file_number}/{groups[1]}_',
            }
        }
        writer = FilebaseBaseWriter(writer_config, meta(),
                                    export_formatter=JsonExportFormatter(dict()))
        writer.close()
        with self.assertRaisesRegexp(KeyError, 'filebase option should not contain'):
            writer.create_filebase_name(('g1', 'g2'), file_name='filename')


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
            }
        }

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()

    def tearDown(self):
        try:
            shutil.rmtree(self.tmp_dir)
        except OSError:
            pass

    def test_get_file_number(self):
        writer_config = self.get_writer_config()
        writer = FSWriter(writer_config, meta(),
                          export_formatter=JsonExportFormatter(dict()))
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
        writer = FSWriter(writer_config, meta(),
                          export_formatter=JsonExportFormatter(dict()))
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

    def test_compression_zip_format(self):
        writer_config = self.get_writer_config()
        writer_config['options'].update({'compression': 'zip'})
        writer = FSWriter(writer_config, meta(),
                          export_formatter=JsonExportFormatter(dict()))
        try:
            writer.write_batch(self.get_batch())
            writer.flush()

        finally:
            writer.close()
        expected_file = '{}/exporter_test0000.jl.zip'.format(self.tmp_dir)
        self.assertTrue(expected_file in writer.written_files)

        import zipfile
        written = []
        with zipfile.ZipFile(expected_file) as z:
            with z.open('exporter_test0000.jl') as f:
                for line in f:
                    written.append(json.loads(line))
        self.assertEqual(written, self.get_batch())

    def test_compression_bz2_format(self):
        writer_config = self.get_writer_config()
        writer_config['options'].update({'compression': 'bz2'})
        writer = FSWriter(writer_config, meta(),
                          export_formatter=JsonExportFormatter(dict()))
        try:
            writer.write_batch(self.get_batch())
            writer.flush()

        finally:
            writer.close()
        expected_file = '{}/exporter_test0000.jl.bz2'.format(self.tmp_dir)
        self.assertTrue(expected_file in writer.written_files)

        written = []
        with bz2.BZ2File(expected_file, 'r') as fin:
            for line in fin:
                written.append(json.loads(line))
        self.assertEqual(written, self.get_batch())

    def test_invalid_compression_format(self):
        options = self.get_writer_config()
        options['options']['compression'] = 'unknown'
        self.assertRaisesRegexp(ConfigurationError,
                                'The compression format can only be '
                                'one of the following:',
                                FilebaseBaseWriter,
                                options,
                                meta())

    def test_get_file_number_with_date(self):
        file_path = '/tmp/%Y%m%d/'
        file_name = '{file_number}_exporter_test_%m%d%y'
        start_file_count = 1
        writer_config = self.get_writer_config()
        writer_config.update({'options': {
            'filebase': file_path + file_name,
            'start_file_count': start_file_count
        }})
        writer = FSWriter(writer_config, meta(),
                          export_formatter=JsonExportFormatter(dict()))
        try:
            writer.write_batch(self.get_batch())
            writer.flush()

        finally:
            writer.close()
        file_path = datetime.datetime.now().strftime(file_path).format(file_number=start_file_count)
        file_name = datetime.datetime.now().strftime(file_name).format(file_number=start_file_count)
        self.assertIn(file_path + file_name + '.jl.gz', writer.written_files)

    def test_check_writer_consistency(self):
        # given
        options = self.get_writer_config()
        options['options']['check_consistency'] = True

        # when:
        writer = FSWriter(options, meta(),
                          export_formatter=JsonExportFormatter(dict()))
        try:
            writer.write_batch(self.get_batch())
            writer.flush()

        finally:
            writer.close()

        # Consistency check passes
        writer.finish_writing()

        with open(os.path.join(self.tmp_dir, 'exporter_test0000.jl.gz'), 'w'):
            with self.assertRaisesRegexp(InconsistentWriteState, 'Wrong size for file'):
                writer.finish_writing()

        os.remove(os.path.join(self.tmp_dir, 'exporter_test0000.jl.gz'))
        with self.assertRaisesRegexp(InconsistentWriteState, 'file is not present at destination'):
            writer.finish_writing()

    def test_writer_md5_generation(self):
        # given
        options = self.get_writer_config()
        options['options']['generate_md5'] = True

        # when:
        writer = FSWriter(options, meta(),
                          export_formatter=JsonExportFormatter(dict()))
        with closing(writer) as w:
            w.write_batch(self.get_batch())
            w.flush()
            w.finish_writing()

        self.assertTrue(os.path.isfile(os.path.join(self.tmp_dir, 'md5checksum.md5')),
                        "Didn't found an expected md5checksum.md5 file")

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
            BaseRecord(city=u'Madrid', country=u'ES', monument='Royal Palace'),
            BaseRecord(city=u'Valencia', country=u'ES', monument='Torres de Serranos'),
            BaseRecord(city=u'Paris', country=u'FR', monument='Eiffel Tour'),
            BaseRecord(city=u'Paris', country=u'FR', monument='Champ de Mars'),
            BaseRecord(city=u'Paris', country=u'FR', monument='Arc de Triomphe'),
        ]
        grouped_batch = self._build_grouped_batch(
            batch, python_expressions=["item['country']", "item['city']"])

        options = self.get_writer_config()
        options['options']['filebase'] = os.path.join(self.tmp_dir, '{groups[0]}/{groups[1]}/file')
        options['options']['items_per_buffer_write'] = 2
        writer = FSWriter(options=options,
                          metadata=meta(),
                          export_formatter=JsonExportFormatter(dict()))

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
            'FR/Paris/file0001.jl.gz',
        ]
        expected = [os.path.join(self.tmp_dir, f) for f in expected_files]

        def listdir_recursive(path):
            return [os.path.join(d, f)
                    for d, _, fnames in os.walk(path)
                    for f in fnames]

        self.assertEqual(sorted(expected), sorted(listdir_recursive(self.tmp_dir)))
