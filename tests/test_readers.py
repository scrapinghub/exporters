import unittest
from exporters.export_managers.settings import Settings
from exporters.readers.base_reader import BaseReader
from exporters.readers.random_reader import RandomReader


class BaseReaderTest(unittest.TestCase):

    def setUp(self):
        exporter_options =  {
            'log_level': 'DEBUG',
            'logger_name': 'export-pipeline'
        }
        self.reader = BaseReader({})

    def test_get_next_batch_not_implemented(self):
        with self.assertRaises(NotImplementedError):
            self.reader.get_next_batch()

    def test_set_last_position(self):
        self.reader.set_last_position(5)
        self.assertEqual(self.reader.last_position, 5)


class RandomReaderTest(unittest.TestCase):

    def setUp(self):
        self.options = {
            'exporter_options': {
                'log_level': 'DEBUG',
                'logger_name': 'export-pipeline'
            },
            'reader': {
                'name': 'exporters.readers.random_reader.RandomReader',
                'options': {
                    'number_of_items': 1000,
                    'batch_size': 100
                }
            },

        }
        self.reader = RandomReader(self.options)

    def test_get_next_batch(self):
        batch = list(self.reader.get_next_batch())
        self.assertTrue(len(batch) == self.options['reader']['options']['batch_size'])

    def test_get_second_batch(self):
        self.reader.get_next_batch()
        batch = list(self.reader.get_next_batch())
        self.assertTrue(len(batch) == self.options['reader']['options']['batch_size'])

    def test_get_all(self):
        total_items = 0
        while not self.reader.finished:
            batch = list(self.reader.get_next_batch())
            total_items += len(batch)
        self.assertTrue(total_items == self.options['reader']['options']['number_of_items'])

    def test_set_last_position_none(self):
        self.reader.set_last_position(0)
        self.assertTrue(0 == self.reader.last_position)
