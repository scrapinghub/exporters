# -*- coding: utf-8 -*-
import random
import unittest
from exporters.filters.base_filter import BaseFilter
from exporters.filters.key_value_filter import KeyValueFilter
from exporters.filters.key_value_filters import InvalidOperator
from exporters.filters.key_value_regex_filter import KeyValueRegexFilter
from exporters.filters.no_filter import NoFilter
from exporters.records.base_record import BaseRecord

from .utils import meta


class BaseFilterTest(unittest.TestCase):

    def setUp(self):
        self.options = {
            'exporter_options': {
                'log_level': 'DEBUG',
                'logger_name': 'export-pipeline'
            }
        }
        self.filter = BaseFilter(self.options, meta())

    def test_no_filter_configured_raise_notimplemented(self):
        with self.assertRaises(NotImplementedError):
            next(self.filter.filter_batch([{}]))

    def test_should_allow_extend_custom_filter(self):
        class CustomFilter(BaseFilter):
            def filter(self, item):
                return item.get('key') == 1

        myfilter = CustomFilter(self.options, meta())
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
        self.filter = NoFilter(self.options, meta())

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
        self.filter = KeyValueFilter({'options': {'keys': self.keys}}, meta())

    def test_filter_with_key_value(self):
        batch = self.filter.filter_batch(self.batch)
        batch = list(batch)
        self.assertEqual(1, len(batch))
        self.assertEqual('es', dict(batch[0])['country_code'])

    def test_filter_logs(self):
        batch = [
            {
                'country': random.choice(['es', 'uk']),
                'value': random.randint(0, 1000)} for i in range(5000)
        ]
        # No exception should be raised
        self.filter.filter_batch(batch)

    def test_filter_with_nested_key_value(self):
        keys = [
            {'name': 'country.state.city', 'value': 'val'}
        ]
        batch = [
            {'country': {
                'state': {
                    'city': random.choice(['val', 'es', 'uk'])
                }
            }, 'value': random.randint(0, 1000)} for i in range(100)
        ]
        filter = KeyValueFilter({'options': {'keys': keys}}, meta())
        batch = filter.filter_batch(batch)
        batch = list(batch)
        self.assertGreater(len(batch), 0)
        for item in batch:
            self.assertEqual(item['country']['state']['city'], 'val')

    def test_filter_with_nested_key_value_with_comma(self):
        keys = [
            {'name': 'country,state,city', 'value': 'val'}
        ]
        batch = [
            {'country': {
                'state': {
                    'city': random.choice(['val', 'es', 'uk'])
                }
            }, 'value': random.randint(0, 1000)} for i in range(100)
        ]
        filter = KeyValueFilter(
            {'options': {'keys': keys, 'nested_field_separator': ','}}, meta())
        batch = filter.filter_batch(batch)
        batch = list(batch)
        self.assertGreater(len(batch), 0)
        for item in batch:
            self.assertEqual(item['country']['state']['city'], 'val')

    def test_filter_with_no_nested_key(self):
        keys = [
            {'name': 'not_a_key', 'value': 'val'}
        ]
        batch = [
            {'country': {
                'state': {
                    'city': random.choice(['val', 'es', 'uk'])
                }
            }, 'value': random.randint(0, 1000)} for i in range(100)
        ]
        filter = KeyValueFilter(
            {'options': {'keys': keys}}, meta())
        batch = filter.filter_batch(batch)
        batch = list(batch)
        self.assertEqual(len(batch), 0, 'Resulting filtered batch should be empty')


class KeyValueRegexFilterTest(unittest.TestCase):

    def test_filter_batch_with_key_value_regex(self):
        # given:
        items = [
            {'name': 'item1', 'country': u'es'},
            {'name': 'item2', 'country': u'egypt'},
            {'name': 'item3', 'country': u'uk'},
            {'name': 'item4', 'country': u'españa'},
        ]
        batch = [BaseRecord(it) for it in items]

        keys = [{'name': 'country', 'value': 'e[sg]'}]
        regex_filter = KeyValueRegexFilter({'options': {'keys': keys}}, meta())

        # when:
        result = list(regex_filter.filter_batch(batch))

        # then:
        self.assertEqual(['es', 'egypt', u'españa'], [d['country'] for d in result])

    def test_filter_with_nested_key_value(self):
        keys = [
            {'name': 'country.state.city', 'value': 'val'}
        ]
        batch = [
            {'country': {
                'state': {
                    'city': random.choice(['val', 'es', 'uk'])
                }
            }, 'value': random.randint(0, 1000)} for i in range(100)
        ]
        filter = KeyValueRegexFilter({'options': {'keys': keys}}, meta())
        batch = filter.filter_batch(batch)
        batch = list(batch)
        self.assertGreater(len(batch), 0)
        for item in batch:
            self.assertEqual(item['country']['state']['city'], 'val')

    def test_filter_with_nested_key_value_with_comma(self):
        keys = [
            {'name': 'country,state,city', 'value': 'val'}
        ]
        batch = [
            {'country': {
                'state': {
                    'city': random.choice(['val', 'es', 'uk'])
                }
            }, 'value': random.randint(0, 1000)} for i in range(100)
        ]
        filter = KeyValueRegexFilter(
            {'options': {'keys': keys, 'nested_field_separator': ','}}, meta())
        batch = filter.filter_batch(batch)
        batch = list(batch)
        self.assertGreater(len(batch), 0)
        self.assertEqual(['val'] * len(batch), [e['country']['state']['city'] for e in batch])

    def test_filter_with_no_nested_key(self):
        keys = [
            {'name': 'not_a_key', 'value': 'val'}
        ]
        batch = [
            {'country': {
                'state': {
                    'city': random.choice(['val', 'es', 'uk'])
                }
            }, 'value': random.randint(0, 1000)} for i in range(100)
        ]
        filter = KeyValueRegexFilter(
            {'options': {'keys': keys}}, meta())
        batch = filter.filter_batch(batch)
        batch = list(batch)
        self.assertEqual(len(batch), 0, 'Resulting filtered batch should be empty')

    def test_filter_with_nonstring_values(self):
        # given:
        batch = [
            {'address': {'country': None}},
            {'address': {'country': 'US'}},
            {'address': {'country': 3}},
            {'address': {'country': 'BR'}},
        ]
        options = {
            'options': {
                'keys': [{'name': 'address.country', 'value': 'US|3'}]
            }
        }
        filter = KeyValueRegexFilter(options, meta())

        # when:
        result = list(filter.filter_batch(batch))

        # then:
        expected = [
            {'address': {'country': 'US'}},
            {'address': {'country': 3}},
        ]
        self.assertEqual(expected, result)


class KeyValueFiltersTest(unittest.TestCase):

    def setUp(self):
        self.options = {
            'exporter_options': {
                'log_level': 'DEBUG',
                'logger_name': 'export-pipeline'
            }
        }

    def test_filter_with_contains_key_value(self):

        keys = [
            {'name': 'country_code', 'value': ['es', 'us'], 'operator': 'in'}
            ]

        items = [
            {'name': 'item1', 'country_code': 'es'},
            {'name': 'item2', 'country_code': 'us'},
            {'name': 'item3', 'country_code': 'uk'}
        ]
        batch = []
        for item in items:
            record = BaseRecord(item)
            batch.append(record)
        filter = KeyValueFilter({'options': {'keys': keys}}, meta())

        batch = filter.filter_batch(batch)
        batch = list(batch)
        self.assertEqual(2, len(batch))

    def test_filter_with_in_key_value(self):

        keys = [
            {'name': 'country_code', 'value': 'es', 'operator': 'contains'}
            ]

        items = [
            {'name': 'item1', 'country_code': ['es', 'us']},
            {'name': 'item2', 'country_code': ['es', 'us']},
            {'name': 'item3', 'country_code': ['uk']}
        ]
        batch = []
        for item in items:
            record = BaseRecord(item)
            batch.append(record)
        filter = KeyValueFilter({'options': {'keys': keys}}, meta())

        batch = filter.filter_batch(batch)
        batch = list(batch)
        self.assertEqual(2, len(batch))

    def test_filter_with_non_existing_op(self):

        keys = [
            {'name': 'country_code', 'value': ['es', 'us'], 'operator': 'not_an_operator'}
            ]

        items = [
            {'name': 'item1', 'country_code': 'es'},
            {'name': 'item2', 'country_code': 'us'},
            {'name': 'item3', 'country_code': 'uk'}
        ]
        batch = []
        for item in items:
            record = BaseRecord(item)
            batch.append(record)
        with self.assertRaisesRegexp(InvalidOperator, 'operator not valid'):
            KeyValueFilter({'options': {'keys': keys}}, meta())
