import json
import random
import unittest
from exporters.exceptions import ConfigurationError
from exporters.export_formatter.base_export_formatter import BaseExportFormatter
from exporters.export_formatter.csv_export_formatter import CSVExportFormatter
from exporters.export_formatter.json_export_formatter import JsonExportFormatter
from exporters.records.base_record import BaseRecord


class BaseExportFormatterTest(unittest.TestCase):

    def setUp(self):
        self.options = {
            'exporter_options': {

            }
        }
        self.export_formatter = BaseExportFormatter(self.options)

    def test_format_not_implemented(self):
        with self.assertRaises(NotImplementedError):
            self.export_formatter.format([])


class JsonFormatterTest(unittest.TestCase):

    def setUp(self):
        self.options = {
            'exporter_options': {

            }
        }
        self.export_formatter = JsonExportFormatter(self.options)

    def test_format(self):
        item = BaseRecord()
        item['key'] = 0
        item['value'] = random.randint(0, 10000)
        item = self.export_formatter.format([item])
        item = list(item)[0]
        self.assertIsInstance(json.loads(item.formatted), dict)

    def test_raise_exception(self):
        with self.assertRaises(Exception):
            list(self.export_formatter.format([1, 2, 3]))


class CSVFormatterTest(unittest.TestCase):

    def setUp(self):
        self.batch = [
            BaseRecord({'key1': 'value1', 'key2': 'value2'}),
            BaseRecord({'key1': 'value1', 'key2': 'value2'}),
            BaseRecord({'key1': 'value1', 'key2': 'value2'}),
            BaseRecord({'key1': 'value1', 'key2': 'value2'}),
            BaseRecord({'key1': 'value1', 'key2': 'value2'})
        ]

    def test_create_without_options_raises_errors(self):
        with self.assertRaisesRegexp(ConfigurationError, "requires at least one of"):
            CSVExportFormatter({})

    def test_format_batch_titles(self):
        options = {
            'options': {
                'show_titles': True,
                'fields': ['key1']
            }
        }
        formatter = CSVExportFormatter(options)
        items = formatter.format(self.batch)
        items = list(items)
        self.assertEqual(items[0].formatted, '"key1"')
        self.assertEqual(items[1].formatted, '"value1"')

    def test_format_batch_no_titles(self):
        # given:
        options = {
            'options': {
                'fields': ['key1']
            }
        }
        formatter = CSVExportFormatter(options)

        # when:
        items = list(formatter.format(self.batch))

        # then:
        self.assertEqual(items[0].formatted, '"key1"')
        self.assertEqual(items[1].formatted, '"value1"')

    def test_format_from_schema(self):
        # given:
        options = {
            'options': {
                'show_titles': True,
                'schema': {
                    '$schema': 'http://json-schema.org/draft-04/schema',
                    'properties': {
                        'key1': {
                            'type': 'string'
                        },
                        'key2': {
                            'type': 'string'
                        }
                    },
                    'required': [
                        'key1',
                        'key2'
                    ],
                    'type': 'object'
                }
            }
        }
        formatter = CSVExportFormatter(options)

        # when:
        items = list(formatter.format(self.batch))

        # print [i.formatted for i in items]
        #
        # then:
        self.assertEqual([True] + [False] * 5, [i.header for i in items])
        expected_titles = set(['"key1"','"key2"'])
        given_titles = set(items[0].formatted.split(','))
        items.pop(0)
        self.assertEqual(expected_titles, given_titles)
        for i in range(5):
            self.assertEqual(set(['"value1"', '"value2"']), set(items[i].formatted.split(',')))
        self.assertEqual(set(['csv']), set(i.format for i in items))
