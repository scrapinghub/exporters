import random
import unittest
from exporters.export_managers.settings import Settings
from exporters.records.base_record import BaseRecord
from exporters.writers.base_writer import BaseWriter
from exporters.writers.console_writer import ConsoleWriter


class BaseWriterTest(unittest.TestCase):

    def setUp(self):
        self.options = {
            'exporter_options': {
                'log_level': 'DEBUG',
                'logger_name': 'export-pipeline'
            },
        }
        self.settings = Settings(self.options['exporter_options'])
        self.writer = BaseWriter({}, self.settings)

    def test_write_not_implemented(self):
        with self.assertRaises(NotImplementedError):
            self.writer.write('', '')


class ConsoleWriterTest(unittest.TestCase):

    def setUp(self):
        self.options = {
            'exporter_options': {
                'log_level': 'DEBUG',
                'logger_name': 'export-pipeline'
            },
        }
        self.settings = Settings(self.options['exporter_options'])
        self.writer = ConsoleWriter({}, self.settings)

    def test_write_console(self):
        items_to_write = []
        for i in range(0, 10):
            item = BaseRecord()
            item['key'] = i
            item['value'] = random.randint(0, 10000)
            items_to_write.append(item)

        self.writer.write_batch(items_to_write)
