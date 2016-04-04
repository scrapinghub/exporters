import unittest
from exporters.readers import FSReader

from .utils import meta


class FSReaderTest(unittest.TestCase):
    def setUp(self):
        self.options = {
            'name': 'exporters.readers.fs_reader.FSReader',
            'options': {
                'path': './tests/data/fs_reader_test',
            }
        }

        self.options_pointer = {
            'name': 'exporters.readers.fs_reader.FSReader',
            'options': {
                'path_pointer': './tests/data/fs_reader_pointer',
            }
        }

        self.options_empty_folder = {
            'name': 'exporters.readers.fs_reader.FSReader',
            'options': {
                'path': './tests/data/fs_reader_empty_folder',
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
