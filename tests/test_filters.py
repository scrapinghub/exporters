# -*- coding: utf-8 -*-
import random
import unittest
from exporters.filters.base_filter import BaseFilter
from exporters.filters.dupe_filter import DupeFilter
from exporters.filters.key_value_filter import KeyValueFilter
from exporters.filters.key_value_filters import InvalidOperator
from exporters.filters.key_value_regex_filter import KeyValueRegexFilter
from exporters.filters.no_filter import NoFilter
from exporters.filters.multiple_filter import MultipleFilter
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


class DupeFilterTest(unittest.TestCase):

    def test_filter_duplicates_with_default_key(self):
        keys = ['8062219f00c79c88', '1859834d918981df', 'e2abb7b480edf910']
        items = [
            {'_key': keys[0], 'name': 'item1', 'country_code': 'es'},
            {'_key': keys[0], 'name': 'item2', 'country_code': 'es'},
            {'_key': keys[1], 'name': 'item3', 'country_code': 'us'},
            {'_key': keys[1], 'name': 'item4', 'country_code': 'us'},
            {'_key': keys[2], 'name': 'item5', 'country_code': 'uk'},
            {'_key': keys[2], 'name': 'item6', 'country_code': 'uk'}
        ]
        batch = []
        for item in items:
            record = BaseRecord(item)
            batch.append(record)
        filter = DupeFilter({'options': {}}, meta())

        batch = filter.filter_batch(batch)
        batch = list(batch)
        self.assertEqual(3, len(batch))
        self.assertEquals(set(keys), set([item['_key'] for item in batch]))
        self.assertEquals(set(['item1', 'item3', 'item5']),
                          set([item['name'] for item in batch]))

    def test_filter_duplicates_with_custom_key(self):
        keys = ['8062219f00c79c88', '1859834d918981df', 'e2abb7b480edf910']
        items = [
            {'custom_key': keys[0], 'name': 'item1', 'country_code': 'es'},
            {'custom_key': keys[0], 'name': 'item2', 'country_code': 'es'},
            {'custom_key': keys[1], 'name': 'item3', 'country_code': 'us'},
            {'custom_key': keys[1], 'name': 'item4', 'country_code': 'us'},
            {'custom_key': keys[2], 'name': 'item5', 'country_code': 'uk'},
            {'custom_key': keys[2], 'name': 'item6', 'country_code': 'uk'}
        ]

        batch = []
        for item in items:
            record = BaseRecord(item)
            batch.append(record)
        filter = DupeFilter({'options': {'key_field': 'custom_key'}}, meta())

        batch = filter.filter_batch(batch)
        batch = list(batch)
        self.assertEqual(3, len(batch))
        self.assertEquals(set(keys),
                          set([item['custom_key'] for item in batch]))
        self.assertEquals(set(['item1', 'item3', 'item5']),
                          set([item['name'] for item in batch]))

    def test_filter_duplicates_empty_key_dont_get_filtered(self):
        items = [
            {'_key': '', 'name': 'item1', 'country_code': 'es'},
            {'_key': '', 'name': 'item2', 'country_code': 'us'},
            {'_key': '', 'name': 'item3', 'country_code': 'uk'}
        ]
        batch = []
        for item in items:
            record = BaseRecord(item)
            batch.append(record)
        filter = DupeFilter({'options': {}}, meta())

        batch = filter.filter_batch(batch)
        batch = list(batch)
        self.assertEqual(3, len(batch))

    def test_filter_duplicates_items_without_keys_dont_get_filtered(self):
        items = [
            {'name': 'item1', 'country_code': 'es'},
            {'name': 'item2', 'country_code': 'us'},
            {'name': 'item3', 'country_code': 'uk'}
        ]
        batch = []
        for item in items:
            record = BaseRecord(item)
            batch.append(record)
        filter = DupeFilter({'options': {}}, meta())

        batch = filter.filter_batch(batch)
        batch = list(batch)
        self.assertEqual(3, len(batch))


class FilterTrue(BaseFilter):
    def filter(self, item):
        return True


class FilterFalse(BaseFilter):
    def filter(self, item):
        return False


class MultipleFilterTest(unittest.TestCase):

    def setUp(self):
        self.items = [
            {'name': 'item1', 'country_code': 'es'},
            {'name': 'item2', 'country_code': 'us'},
        ]
        self.false = {"name": "tests.test_filters.FilterFalse"}
        self.true = {"name": "tests.test_filters.FilterTrue"}

    def test_multiple_filter_should_be_able_to_load_filters(self):
        filter_options = {"filters": [self.true, self.false]}
        loaded_filters = MultipleFilter(
            {'options': filter_options}, meta()).filters
        self.assertEqual(len(loaded_filters), 2)
        self.assertIsInstance(loaded_filters[0].values()[0], FilterTrue)
        self.assertIsInstance(loaded_filters[1].values()[0], FilterFalse)

    def test_and_true_true(self):
        filter_options = {"filters": [self.true, self.true]}
        filter = MultipleFilter({'options': filter_options}, meta())
        self.assertTrue(filter.filter(self.items))

    def test_and_true_false(self):
        filter_options = {"filters": [self.true, self.false]}
        filter = MultipleFilter({'options': filter_options}, meta())
        self.assertFalse(filter.filter(self.items))

    def test_and_false_false(self):
        filter_options = {"filters": [self.false, self.false]}
        filter = MultipleFilter({'options': filter_options}, meta())
        self.assertFalse(filter.filter(self.items))

    def test_true_or_false_is_true(self):
        filter_options = {"filters": [{'or': [self.true, self.true]}]}
        filter = MultipleFilter({'options': filter_options}, meta())
        self.assertTrue(filter.filter(self.items))

    def test_true_or_true_is_true(self):
        filter_options = {"filters": [{'or': [self.true, self.true]}]}
        filter = MultipleFilter({'options': filter_options}, meta())
        self.assertTrue(filter.filter(self.items))

    def test_false_or_false_is_false(self):
        filter_options = {"filters": [{'or': [self.false, self.false]}]}
        filter = MultipleFilter({'options': filter_options}, meta())
        self.assertFalse(filter.filter(self.items))

    def test_complex_and_or_logic(self):

        filter_options = {"filters": [
            self.false,
            {'or': [self.true, self.false]}
        ]}
        filter = MultipleFilter({'options': filter_options}, meta())
        print filter_options
        self.assertFalse(filter.filter(self.items))

        for no_of_filters in xrange(1, 5):
            filter_options = {"filters": [self.true] * no_of_filters + [
                {'or': [self.true, self.false]}
            ]}
            filter = MultipleFilter({'options': filter_options}, meta())
            print filter_options
            self.assertTrue(filter.filter(self.items))

            filter_options = {
                "filters": [self.true] * no_of_filters + [
                    {'or': [self.false, self.false]}
                ]
            }
            filter = MultipleFilter({'options': filter_options}, meta())
            self.assertFalse(filter.filter(self.items))

            filter_options = {
                "filters": [self.true] * no_of_filters + [
                    {'or': [self.true, self.true]}
                ]
            }
            filter = MultipleFilter({'options': filter_options}, meta())
            self.assertTrue(filter.filter(self.items))

            filter_options = {
                "filters": [self.true] * no_of_filters + [
                    {'or': [self.false, self.false, self.true]}
                ]
            }
            filter = MultipleFilter({'options': filter_options}, meta())
            self.assertTrue(filter.filter(self.items))

    def test_complex_only_or_expressions(self):
        filter_options = {"filters": [
            {'or': [self.true, self.false, self.false]}]}
        filter = MultipleFilter({'options': filter_options}, meta())
        self.assertTrue(filter.filter(self.items))

        filter_options = {"filters": [
            {'or': [self.false, self.false, self.true]}]}
        filter = MultipleFilter({'options': filter_options}, meta())
        self.assertTrue(filter.filter(self.items))

        filter_options = {"filters": [{'or': [self.false] * 3}]}
        filter = MultipleFilter({'options': filter_options}, meta())
        self.assertFalse(filter.filter(self.items))

        filter_options = {"filters": [
            {'or': [self.true, self.false]},
            {'or': [self.false, self.true]},
        ]}
        filter = MultipleFilter({'options': filter_options}, meta())
        self.assertTrue(filter.filter(self.items))
