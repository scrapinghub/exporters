import json
import random
import unittest
from exporters.export_formatter.base_export_formatter import BaseExportFormatter
from exporters.export_formatter.csv_export_formatter import CSVExportFormatter
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


class CSVFormatterTest(unittest.TestCase):

    def setUp(self):
        self.settings = Settings({})
        self.batch = [
            BaseRecord({'key1': 'value1', 'key2': 'value2'}),
            BaseRecord({'key1': 'value1', 'key2': 'value2'}),
            BaseRecord({'key1': 'value1', 'key2': 'value2'}),
            BaseRecord({'key1': 'value1', 'key2': 'value2'}),
            BaseRecord({'key1': 'value1', 'key2': 'value2'})
        ]

    def test_show_titles_different_sizes(self):
        options = {
            'show_titles': True,
            'columns': ['col1', 'col2'],
            'titles': ['title1']
        }
        with self.assertRaises(ValueError):
            CSVExportFormatter(options, self.settings)

    def test_format_batch_default(self):
        options = {

        }
        formatter = CSVExportFormatter(options, self.settings)
        items = formatter.format(self.batch)
        for item in items:
            self.assertIsInstance(item.formatted, basestring)
            self.assertEqual(len(item.formatted.split(',')), 2)

    def test_format_batch_titles(self):
        options = {
            'show_titles': True,
            'columns': ['key1', 'key1'],
            'titles': ['title1', 'title2']
        }
        formatter = CSVExportFormatter(options, self.settings)
        items = formatter.format(self.batch)
        for item in items:
            if isinstance(item, basestring):
                title1, title2 = item.replace('\n', '').split(',')
                self.assertEqual(title1, 'title1')
                self.assertEqual(title2, 'title2')
            else:
                self.assertIsInstance(item.formatted, basestring)
                self.assertEqual(len(item.formatted.split(',')), 2)
