import unittest
from exporters.readers import FSReader

from .utils import meta


class FSReaderTest(unittest.TestCase):
    def setUp(self):
        self.options = {
            'name': 'exporters.readers.fs_reader.FSReader',
            'options': {
                'input': {
                    'dir': './tests/data/fs_reader_test',
                }
            }
        }

        self.options_pointer = {
            'name': 'exporters.readers.fs_reader.FSReader',
            'options': {
                'input': {
                    'dir_pointer': './tests/data/fs_reader_pointer',
                }
            }
        }

        self.options_empty_folder = {
            'name': 'exporters.readers.fs_reader.FSReader',
            'options': {
                'input': {
                    'dir': './tests/data/fs_reader_empty_folder',
                }
            }
        }

    def test_read_from_folder(self):
        expected = [{u'item': u'value1'}, {u'item': u'value2'}, {u'item': u'value3'}]
        reader = FSReader(self.options, meta())
        reader.set_last_position(None)
        batch = list(reader.get_next_batch())
        self.assertEqual(expected, batch)

    def test_read_from_pointer(self):
        expected = [{u'item': u'value1'}, {u'item': u'value2'}, {u'item': u'value3'}]
        reader = FSReader(self.options_pointer, meta())
        reader.set_last_position(None)
        batch = list(reader.get_next_batch())
        self.assertEqual(expected, batch)

    def test_read_from_empty_folder(self):
        reader = FSReader(self.options_empty_folder, meta())
        list(reader.get_next_batch())
        self.assertTrue(reader.is_finished())

    @staticmethod
    def _make_fs_reader(options):
        full_config = {
            'name': 'exporters.readers.fs_reader.FSReader',
            'options': options
        }
        reader = FSReader(full_config, meta())
        reader.set_last_position(None)
        return reader

    def test_read_from_file(self):
        reader = self._make_fs_reader({
            'input': './tests/data/fs_reader_test/fs_test_data.jl.gz',
        })
        batch = list(reader.get_next_batch())
        expected = [{u'item': u'value1'}, {u'item': u'value2'}, {u'item': u'value3'}]
        self.assertEqual(expected, batch)

    def test_read_from_multiple_files(self):
        reader = self._make_fs_reader({
            'input': [
                './tests/data/fs_reader_test/fs_test_data.jl.gz',
                './tests/data/fs_reader_test/fs_test_data.jl.gz',
            ]
        })
        batch = list(reader.get_next_batch())
        expected = [{u'item': u'value1'}, {u'item': u'value2'}, {u'item': u'value3'}] * 2
        self.assertEqual(expected, batch)

    def test_read_from_file_and_dir(self):
        reader = self._make_fs_reader({
            'input': [
                './tests/data/fs_reader_test/fs_test_data.jl.gz',
                {'dir': './tests/data/fs_reader_test'}
            ]
        })
        batch = list(reader.get_next_batch())
        expected = [{u'item': u'value1'}, {u'item': u'value2'}, {u'item': u'value3'}] * 2
        self.assertEqual(expected, batch)
