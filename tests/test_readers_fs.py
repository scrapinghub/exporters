import unittest
from exporters.readers import FSReader


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

    def test_read_from_folder(self):
        expected = [{u'item': u'value1'}, {u'item': u'value2'}, {u'item': u'value3'}]
        reader = FSReader(self.options)
        reader.set_last_position(None)
        batch = list(reader.get_next_batch())
        self.assertEqual(expected, batch)

    def test_read_from_pointer(self):
        expected = [{u'item': u'value1'}, {u'item': u'value2'}, {u'item': u'value3'}]
        reader = FSReader(self.options_pointer)
        reader.set_last_position(None)
        batch = list(reader.get_next_batch())
        self.assertEqual(expected, batch)
