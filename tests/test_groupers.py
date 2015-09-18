import random
import unittest
from exporters.groupers.base_grouper import BaseGrouper
from exporters.groupers.file_key_grouper import FileKeyGrouper
from exporters.records.base_record import BaseRecord


class BaseGrouperTest(unittest.TestCase):

    def setUp(self):
        self.options = {
            'exporter_options': {
                'LOG_LEVEL': 'DEBUG',
                'LOGGER_NAME': 'export-pipeline'
            },
            "writer": {
                'grouper': {
                    'name': 'exporters.groupers.base_grouper.BaseGrouper',
                    'options': {
                        'keys': ['country_code', 'state', 'city']
                    }
                }
            }
        }

    def test_raise_exception(self):
        grouper = BaseGrouper(self.options)
        with self.assertRaises(NotImplementedError):
            grouper.group_batch([])


class FileKeyGrouperTest(unittest.TestCase):

    country_codes = ['es', 'uk', 'us']
    states = ['valencia', 'madrid', 'barcelona']
    cities = ['alicante', 'lleida', 'somecity']

    def _get_batch(self, batch_size=1000):

        batch = []
        for i in range(0, batch_size):
            item = BaseRecord()
            item['key'] = i
            item['country_code'] = random.choice(self.country_codes)
            item['state'] = random.choice(self.states)
            item['city'] = random.choice(self.cities)
            item['value'] = random.randint(0, 10000)
            batch.append(item)
        return batch

    def setUp(self):
        self.options = {
            'exporter_options': {
                'LOG_LEVEL': 'DEBUG',
                'LOGGER_NAME': 'export-pipeline'
            },

            'grouper': {
                'name': 'exporters.groupers.file_key_grouper.FileKeyGrouper',
                'options': {
                    'keys': ['country_code', 'state', 'city']
                }
            }

        }

    def test_group_batch(self):
        grouper = FileKeyGrouper(self.options['grouper'])
        batch = self._get_batch()
        grouped = grouper.group_batch(batch)
        for item in grouped:
            country, state, city = item.group_membership
            self.assertTrue(country in self.country_codes)
            self.assertTrue(state in self.states)
            self.assertTrue(city in self.cities)

    def test_unknown_keys_batch(self):
        g_opts = {
            'name': 'exporters.groupers.file_key_grouper.FileKeyGrouper',
            'options': {
                'keys': ['country_code', 'not_a_key', 'city']
            }
        }
        grouper = FileKeyGrouper(g_opts)
        batch = self._get_batch()
        grouped = grouper.group_batch(batch)
        for item in grouped:
            country, state, city = item.group_membership
            self.assertTrue(state == 'unknown')
