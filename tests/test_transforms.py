from __future__ import absolute_import
import unittest
from collections import OrderedDict
from exporters.records.base_record import BaseRecord
from exporters.transform.base_transform import BaseTransform
from exporters.transform.no_transform import NoTransform
from exporters.transform.pythonexp_transform import PythonexpTransform
from exporters.transform.flatson_transform import FlatsonTransform
from exporters.module_loader import ModuleLoader

from .utils import meta


class BaseTransformTest(unittest.TestCase):
    def setUp(self):
        self.options = {
            'exporter_options': {
                'log_level': 'DEBUG',
                'logger_name': 'export-pipeline'
            },
            'reader': {
                'name': 'exporters.readers.random_reader.RandomReader',
                'options': {
                    'number_of_items': 1000,
                    'batch_size': 100
                }
            },
        }

        self.transform = BaseTransform(self.options)

    def test_raise_exception(self):
        with self.assertRaises(NotImplementedError):
            self.transform.transform_batch([])


class NoTransformTest(unittest.TestCase):
    def setUp(self):
        self.options = {
            'exporter_options': {
                'log_level': 'DEBUG',
                'logger_name': 'export-pipeline'
            },
            'reader': {
                'name': 'exporters.readers.random_reader.RandomReader',
                'options': {
                    'number_of_items': 1000,
                    'batch_size': 100
                }
            },
        }

        self.transform = NoTransform(self.options)

    def test_transform_empty_batch(self):
        self.assertEquals(self.transform.transform_batch([]), [])

    def test_transform_batch(self):
        reader = ModuleLoader().load_reader(self.options['reader'], meta())
        # FIXME inline batch, without a reader
        batch = reader.get_next_batch()
        self.assertEquals(self.transform.transform_batch(batch), batch)


class PythonexpTransformTest(unittest.TestCase):
    def setUp(self):
        self.options = {
            'exporter_options': {
                'log_level': 'DEBUG',
                'logger_name': 'export-pipeline'
            },
        }

        self.batch = [
            BaseRecord({'name': 'item1', 'country_code': 'es'}),
            BaseRecord({'name': 'item2', 'country_code': 'uk'})
        ]

        self.transform = PythonexpTransform({'options': {'python_expressions': [
            "item.update({'new_field': item.get('country_code')+'-'+item.get('name')})"]}})

    def test_transform_empty_batch(self):
        self.assertEqual(list(self.transform.transform_batch([])), [])

    def test_transform_batch(self):
        for item in list(self.transform.transform_batch(self.batch)):
            self.assertIn('country_code', item)
            self.assertIn('name', item)
            self.assertIn('new_field', item)
            self.assertEqual(item['new_field'], item['country_code'] + '-' + item['name'])


class FlatsonTransformTest(unittest.TestCase):
    def setUp(self):
        self.options = {
            'exporter_options': {
                'log_level': 'DEBUG',
                'logger_name': 'export-pipeline'
            },
        }

        self.batch = [
            BaseRecord({
                "name": "Claudio", "age": 42,
                "address": {"city": "Paris", "street": "Rue de Sevres"},
                "skills": ["hacking", "soccer"]}),
            BaseRecord({
                "name": "Marcelo", "age": 22,
                "address": {"city": "Sao Paulo", "street": "25 de Marco"},
                "skills": ["guitar", "piano"]}),
        ]

        self.transform = FlatsonTransform(
            {'options': {
                'flatson_schema': {
                    "$schema": "http://json-schema.org/draft-04/schema",
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "age": {"type": "number"},
                        "address": {
                            "type": "object",
                            "properties": {
                                "city": {"type": "string"},
                                "street": {"type": "string"}
                            }
                        },
                        "skills": {
                            "type": "array", "items": {"type": "string"}
                        }
                    }
                }
            }}
        )

    def test_transform_empty_batch(self):
        self.assertEqual(list(self.transform.transform_batch([])), [])

    def test_transform_batch(self):
        result = list(self.transform.transform_batch(self.batch))
        expected = [
            OrderedDict(
                [(u'address.city', 'Paris'),
                 (u'address.street', 'Rue de Sevres'), ('age', 42),
                 ('name', 'Claudio'), ('skills', u'["hacking","soccer"]')]),
            OrderedDict(
                [(u'address.city', 'Sao Paulo'),
                 (u'address.street', '25 de Marco'), ('age', 22),
                 ('name', 'Marcelo'), ('skills', u'["guitar","piano"]')])
        ]
        self.assertEqual(result, expected)
