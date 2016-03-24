import datetime
import unittest

from exporters.filters.pythonexp_filter import PythonexpFilter
from exporters.records.base_record import BaseRecord

from .utils import meta


class PythonexpFilterFilterTest(unittest.TestCase):

    def test_filter_batch_with_python_expression(self):
        batch = [
            BaseRecord({'name': 'item1', 'country_code': 'es'}),
            BaseRecord({'name': 'item2', 'country_code': 'uk'}),
        ]
        python_filter = PythonexpFilter(
            {'options': {'python_expression': 'item[\'country_code\']==\'uk\''}},
            meta()
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
        expr = ("item.get('updated') and item['updated'] >= "
                "str(datetime.datetime.now() - datetime.timedelta(days=1))[:10]")
        python_filter = PythonexpFilter(
            {'options': {'python_expression': expr}}, meta())
        result = list(python_filter.filter_batch(batch))
        self.assertEqual(['item2', 'item3'],
                         [d['name'] for d in result])

    def test_filter_with_fuzzywuzzy(self):
        batch = [
            BaseRecord({'name': 'Bilbao Falcons', 'country_code': 'es'}),
            BaseRecord({'name': 'New York Jets', 'country_code': 'us'}),
            BaseRecord({'name': 'Madrid Cabbaleros', 'country_code': 'es'}),
            BaseRecord({'name': 'New York Giants', 'country_code': 'us'}),
        ]
        expr = "fuzz.ratio('New York', item.get('name')) > 50"
        python_filter = PythonexpFilter(
            {'options': {
                'python_expression': expr,
                'imports': {'fuzz': 'fuzzywuzzy.fuzz'}
            }},
            meta()
        )
        result = list(python_filter.filter_batch(batch))
        self.assertEqual(['New York Jets', 'New York Giants'],
                         [d['name'] for d in result])
