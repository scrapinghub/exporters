import json
import io
import csv
import random
import unittest
from ozzy.exceptions import ConfigurationError
from ozzy.export_formatter.base_export_formatter import BaseExportFormatter
from ozzy.export_formatter.csv_export_formatter import CSVExportFormatter
from ozzy.export_formatter.json_export_formatter import JsonExportFormatter
from ozzy.records.base_record import BaseRecord
from tests.utils import meta


class BaseExportFormatterTest(unittest.TestCase):

    def setUp(self):
        self.options = {

        }
        self.export_formatter = BaseExportFormatter(self.options)

    def test_format_not_implemented(self):
        with self.assertRaises(NotImplementedError):
            self.export_formatter.format({})


class JsonFormatterTest(unittest.TestCase):

    def setUp(self):
        self.options = {

        }
        self.export_formatter = JsonExportFormatter(self.options, meta())

    def test_format(self):
        item = BaseRecord()
        item['key'] = 0
        item['value'] = random.randint(0, 10000)
        item = self.export_formatter.format(item)
        self.assertIsInstance(json.loads(item), dict)


class CSVFormatterTest(unittest.TestCase):

    def setUp(self):
        self.batch = [
            BaseRecord({'key1': 'value1', 'key2': "valu'e2"}),
            BaseRecord({'key1': 'value1', 'key2': 'valu"e2'}),
            BaseRecord({'key1': 'value1', 'key2': 'valu|e2'}),
            BaseRecord({'key1': 'value1', 'key2': 'value2'}),
            BaseRecord({'key1': 'value1', 'key2': 'value2'})
        ]

    def test_create_without_options_raises_errors(self):
        with self.assertRaisesRegexp(ConfigurationError, "requires at least one of"):
            CSVExportFormatter({})

    def test_format_batch_and_load_file(self):
        # given:
        options = {
            'options': {
                'fields': ['key1', 'key2']
            }
        }
        formatter = CSVExportFormatter(options)

        # when:
        formatted_batch = [formatter.format(item) for item in self.batch]

        # then:
        memfile = self._create_memfile((it for it in formatted_batch), header=['"key1","key2"'])
        self.assertEqual(self.batch, list(csv.DictReader(memfile)))

    def test_format_batch_no_show_titles(self):
        # given:
        fields = ['key1', 'key2']
        options = {
            'options': {
                'show_titles': False,
                'fields': fields,
            }
        }
        formatter = CSVExportFormatter(options)

        # when:
        formatted_batch = [formatter.format(item) for item in self.batch]

        # then:
        memfile = self._create_memfile(it for it in formatted_batch)
        self.assertEqual(self.batch, list(csv.DictReader(memfile, fieldnames=fields)))

    def test_format_batch_with_custom_delimiter(self):
        # given:
        options = {
            'options': {
                'fields': ['key1', 'key2'],
                'delimiter': '|',
                'show_titles': True
            }
        }
        formatter = CSVExportFormatter(options)

        # when:
        formatted_batch = [formatter.format(item) for item in self.batch]

        # then:
        memfile = self._create_memfile((it for it in formatted_batch), header=['"key1"|"key2"'])

        self.assertEqual(self.batch, list(csv.DictReader(memfile, delimiter='|')))

    def test_format_from_schema(self):
        # given:
        options = {
            'options': {
                'show_titles': True,
                'schema': {
                    '$schema': 'http://json-schema.org/draft-04/schema',
                    'properties': {
                        'key1': {'type': 'string'},
                        'key2': {'type': 'string'}
                    },
                    'required': ['key1'],
                    'type': 'object'
                }
            }
        }
        formatter = CSVExportFormatter(options)

        # when:
        formatted_batch = [formatter.format(item) for item in self.batch]

        # then:
        memfile = self._create_memfile((it for it in formatted_batch), header=['"key1","key2"'])
        self.assertEqual(self.batch, list(csv.DictReader(memfile)))

    def _create_memfile(self, lines, header=None):
        if not header:
            header = []
        lines = header + list(lines)
        return io.BytesIO('\n'.join(l for l in lines))
