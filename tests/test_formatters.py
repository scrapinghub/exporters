import json
import random
import unittest
from exporters.export_formatter.base_export_formatter import BaseExportFormatter
from exporters.export_formatter.json_export_formatter import JsonExportFormatter
from exporters.export_managers.settings import Settings
from exporters.records.base_record import BaseRecord




class BaseExportFormatterTest(unittest.TestCase):

    def setUp(self):
        self.options = {
            'exporter_options': {

            }
        }
        self.settings = Settings(self.options['exporter_options'])
        self.export_formatter = BaseExportFormatter(self.options, self.settings)

    def test_format_not_implemented(self):
        with self.assertRaises(NotImplementedError):
            self.export_formatter.format([])


class JsonFormatterTest(unittest.TestCase):

    def setUp(self):
        self.options = {
            'exporter_options': {

            }
        }
        self.settings = Settings(self.options['exporter_options'])
        self.export_formatter = JsonExportFormatter(self.options, self.settings)

    def test_format(self):
        item = BaseRecord()
        item['key'] = 0
        item['value'] = random.randint(0, 10000)
        item = self.export_formatter.format([item])
        item = list(item)[0]
        self.assertIsInstance(json.loads(item.formatted), dict)

    def test_raise_exception(self):
        with self.assertRaises(Exception):
            list(self.export_formatter.format([1,2,3]))
