import unittest
import datetime
from exporters.filters.base_filter import BaseFilter
from exporters.filters.key_value_filter import KeyValueFilter
from exporters.filters.key_value_regex_filter import KeyValueRegexFilter
from exporters.filters.no_filter import NoFilter
from exporters.filters.pythonexp_filter import PythonexpFilter
from exporters.records.base_record import BaseRecord


class BaseFilterTest(unittest.TestCase):

    def setUp(self):
        self.options = {
            'exporter_options': {
                'log_level': 'DEBUG',
                'logger_name': 'export-pipeline'
            }
        }
        self.filter = BaseFilter(self.options)

    def test_no_filter_configured_raise_notimplemented(self):
        with self.assertRaises(NotImplementedError):
            next(self.filter.filter_batch([{}]))

    def test_should_allow_extend_custom_filter(self):
        class CustomFilter(BaseFilter):
            def filter(self, item):
                return item.get('key') == 1

        myfilter = CustomFilter(self.options)
        output = list(myfilter.filter_batch([{'key': 1}, {'key': 2}]))
        self.assertEqual([{'key': 1}], output)


class NoFilterTest(unittest.TestCase):

    def setUp(self):
        self.options = {
            'exporter_options': {
                'log_level': 'DEBUG',
                'logger_name': 'export-pipeline'
            }
        }
        self.filter = NoFilter(self.options)

    def test_filter_empty_batch(self):
        self.assertTrue(self.filter.filter_batch([]) == [])

    def test_filter_batch_no_op(self):
        items = [{'name': 'item1', 'value': 'value1'}, {'name': 'item2', 'value': 'value2'}]
        batch = []
        for item in items:
            record = BaseRecord()
            record.record = item
            batch.append(record)
        self.assertTrue(self.filter.filter_batch(batch) == batch)


class KeyValueFilterTest(unittest.TestCase):

    def setUp(self):
        self.options = {
            'exporter_options': {
                'log_level': 'DEBUG',
                'logger_name': 'export-pipeline'
            }
        }
        self.keys = [
            {'name': 'country_code', 'value': 'es'}
            ]

        items = [{'name': 'item1', 'country_code': 'es'}, {'name': 'item2', 'country_code': 'uk'}]
        self.batch = []
        for item in items:
            record = BaseRecord(item)
            self.batch.append(record)
        self.filter = KeyValueFilter({'options': {'keys': self.keys}})

    def test_filter_with_key_value(self):
        batch = self.filter.filter_batch(self.batch)
        batch = list(batch)
        self.assertEqual(1, len(batch))
        self.assertEqual('es', dict(batch[0])['country_code'])


class KeyValueRegexFilterTest(unittest.TestCase):

    def setUp(self):
        self.options = {
            'exporter_options': {
                'log_level': 'DEBUG',
                'logger_name': 'export-pipeline'
            }
        }
        self.keys = [
            {'name': 'country_code', 'value': 'e'}
            ]

        items = [{'name': 'item1', 'country_code': 'es'}, {'name': 'item2', 'country_code': 'uk'}]
        self.batch = []
        for item in items:
            record = BaseRecord(item)
            self.batch.append(record)
        self.filter = KeyValueRegexFilter({'options': {'keys': self.keys}})

    def test_filter_batch_with_key_value_regex(self):
        batch = self.filter.filter_batch(self.batch)
        batch = list(batch)
        self.assertEqual(1, len(batch))
        self.assertIn('e', dict(batch[0])['country_code'])


class PythonexpFilterFilterTest(unittest.TestCase):

    def test_filter_batch_with_python_expression(self):
        batch = [
            BaseRecord({'name': 'item1', 'country_code': 'es'}),
            BaseRecord({'name': 'item2', 'country_code': 'uk'}),
        ]
        python_filter = PythonexpFilter(
            {'options': {'python_expression': 'item[\'country_code\']==\'uk\''}}
        )
        result = list(python_filter.filter_batch(batch))
        self.assertEqual(1, len(result))
        self.assertEqual('uk', dict(result[0])['country_code'])

    def test_filter_with_datetime(self):
        now = datetime.datetime.now()
        batch = [
            BaseRecord({'name': 'item1', 'updated': str(now - datetime.timedelta(days=2))}),
            BaseRecord({'name': 'item2', 'updated': str(now - datetime.timedelta(days=1))}),
            BaseRecord({'name': 'item3', 'updated': str(now)}),
        ]
        expr = "item.get('updated') and item['updated'] >= str(datetime.datetime.now() - datetime.timedelta(days=1))[:10]"
        python_filter = PythonexpFilter({'options': {'python_expression': expr}})
        result = list(python_filter.filter_batch(batch))
        self.assertEqual(['item2', 'item3'], [d['name'] for d in result])
