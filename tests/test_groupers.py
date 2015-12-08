import random
import unittest
from copy import deepcopy
from exporters.groupers.base_grouper import BaseGrouper
from exporters.groupers.file_key_grouper import FileKeyGrouper
from exporters.groupers.python_exp_grouper import PythonExpGrouper
from exporters.records.base_record import BaseRecord


class BaseGrouperTest(unittest.TestCase):
    country_codes = ['es', 'uk', 'us']
    states = ['valencia', 'madrid', 'barcelona']
    cities = ['alicante', 'lleida', 'somecity']

    def setUp(self):
        self.options = {
            'grouper': {
                'name': 'exporters.groupers.base_grouper.BaseGrouper',
                'options': {
                }
            }
        }

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

    def test_raise_exception(self):
        grouper = BaseGrouper(self.options)
        with self.assertRaises(NotImplementedError):
            grouper.group_batch([])


class FileKeyGrouperTest(BaseGrouperTest):
    def setUp(self):
        super(FileKeyGrouperTest, self).setUp()

        self.options_ok = deepcopy(self.options)
        self.options_ok['grouper']['options'][
            'keys'] = ['country_code', 'state', 'city']

        self.options_unknown_key = deepcopy(self.options)
        self.options_unknown_key['grouper']['options'][
            'keys'] = ['country_code', 'not_a_key', 'city']

    def test_group_batch(self):
        grouper = FileKeyGrouper(self.options_ok['grouper'])
        batch = self._get_batch()
        grouped = grouper.group_batch(batch)
        for item in grouped:
            country, state, city = item.group_membership
            self.assertTrue(country in self.country_codes)
            self.assertTrue(state in self.states)
            self.assertTrue(city in self.cities)

    def test_unknown_keys_batch(self):
        grouper = FileKeyGrouper(self.options_unknown_key['grouper'])
        batch = self._get_batch()
        grouped = grouper.group_batch(batch)
        for item in grouped:
            country, state, city = item.group_membership
            self.assertTrue(state == 'unknown')


class PythonExpGrouperTest(BaseGrouperTest):
    def setUp(self):
        super(PythonExpGrouperTest, self).setUp()

        self.options_exp_in = deepcopy(self.options)
        self.options_exp_in['grouper']['options'][
            'python_expressions'] = ['item[\'country_code\'] in [\'uk\', \'us\']']

        self.options_value_modulo = deepcopy(self.options_exp_in)
        self.options_value_modulo['grouper']['options'][
            'python_expressions'] = ['item[\'value\'] % 5']

        self.options_multiple = deepcopy(self.options_exp_in)
        self.options_multiple['grouper']['options'][
            'python_expressions'] = ['item[\'country_code\'] in [\'uk\', \'us\']', 'item[\'value\'] % 5']

        self.options_invalid = deepcopy(self.options_exp_in)
        self.options_invalid['grouper']['options'][
            'python_expressions'] = ['item[\'description\'] % 5']

    def test_group_batch_is_in(self):
        grouper = PythonExpGrouper(self.options_exp_in['grouper'])
        batch = self._get_batch()
        grouped = grouper.group_batch(batch)
        for item in grouped:
            is_in = item.group_membership[0]
            self.assertTrue((item['country_code'] in ['uk', 'us']) == is_in)

    def test_group_batch_modulo(self):
        grouper = PythonExpGrouper(self.options_value_modulo['grouper'])
        batch = self._get_batch()
        grouped = grouper.group_batch(batch)
        for item in grouped:
            modulo = item.group_membership[0]
            self.assertTrue(item['value'] % 5 == modulo)

    def test_group_batch_multiple(self):
        grouper = PythonExpGrouper(self.options_multiple['grouper'])
        batch = self._get_batch()
        grouped = grouper.group_batch(batch)
        for item in grouped:
            is_in, modulo = item.group_membership
            self.assertTrue((item['country_code'] in ['uk', 'us']) == is_in)
            self.assertTrue(item['value'] % 5 == modulo)

    def test_group_batch_invalid(self):
        grouper = PythonExpGrouper(self.options_invalid['grouper'])
        batch = self._get_batch()
        grouped = grouper.group_batch(batch)
        for item in grouped:
            self.assertRaises(Exception)