import unittest
from exporters.readers.base_reader import BaseReader
from exporters.readers.random_reader import RandomReader

from .utils import meta


class BaseReaderTest(unittest.TestCase):

    def setUp(self):
        self.reader = BaseReader({}, meta())

    def test_get_next_batch_not_implemented(self):
        with self.assertRaises(NotImplementedError):
            self.reader.get_next_batch()

    def test_set_last_position(self):
        self.reader.set_last_position(dict(position=5))
        self.assertEqual(self.reader.last_position, dict(position=5))


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
        self.reader = RandomReader(self.options, meta())
        self.reader.set_last_position(None)

    def test_get_next_batch(self):
        batch = list(self.reader.get_next_batch())
        self.assertEqual(len(batch), self.options['reader']['options']['batch_size'])

    def test_get_second_batch(self):
        self.reader.get_next_batch()
        batch = list(self.reader.get_next_batch())
        self.assertEqual(len(batch), self.options['reader']['options']['batch_size'])
        self.assertEqual(self.reader.get_metadata('read_items'),
                         self.options['reader']['options']['batch_size'])

    def test_get_all(self):
        total_items = 0
        while not self.reader.finished:
            batch = list(self.reader.get_next_batch())
            total_items += len(batch)
        self.assertEqual(total_items, self.options['reader']['options']['number_of_items'])

    def test_set_last_position_none(self):
        self.reader.set_last_position({'last_read': 123})
        self.assertEqual({'last_read': 123}, self.reader.last_position)
