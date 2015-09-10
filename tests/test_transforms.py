import unittest
from exporters.export_managers.settings import Settings
from exporters.records.base_record import BaseRecord
from exporters.transform.base_transform import BaseTransform
from exporters.transform.jq_transform import JQTransform
from exporters.transform.no_transform import NoTransform
from exporters.transform.pythonexp_transform import PythonexpTransform
from exporters.module_loader import ModuleLoader


class BaseTransformTest(unittest.TestCase):
    def setUp(self):
        self.options = {
            'exporter_options': {
                'loglevel': 'DEBUG',
                'loggername': 'export-pipeline'
            },
            'reader': {
                'name': 'exporters.readers.random_reader.RandomReader',
                'options': {
                    'number_of_items': 1000,
                    'batch_size': 100
                }
            },
        }
        self.settings = Settings(self.options['exporter_options'])
        self.transform = BaseTransform(self.options, self.settings)

    def test_raise_exception(self):
        with self.assertRaises(NotImplementedError):
            self.transform.transform_batch([])


class NoTransformTest(unittest.TestCase):
    def setUp(self):
        self.options = {
            'exporter_options': {
                'loglevel': 'DEBUG',
                'loggername': 'export-pipeline'
            },
            'reader': {
                'name': 'exporters.readers.random_reader.RandomReader',
                'options': {
                    'number_of_items': 1000,
                    'batch_size': 100
                }
            },
        }
        self.settings = Settings(self.options['exporter_options'])
        self.transform = NoTransform(self.options, self.settings)

    def test_transform_empty_batch(self):
        self.assertTrue(self.transform.transform_batch([]) == [])

    def test_transform_batch(self):
        reader = ModuleLoader().load_reader(self.options['reader'], self.settings)
        # FIXME inline batch, without a reader
        batch = reader.get_next_batch()
        self.assertTrue(self.transform.transform_batch(batch) == batch)


class JqTransformTest(unittest.TestCase):
    def setUp(self):
        self.options = {
            'exporter_options': {
                'loglevel': 'DEBUG',
                'loggername': 'export-pipeline'
            },
        }

        self.batch = [BaseRecord({'name': 'item1', 'country_code': 'es'}), BaseRecord({'name': 'item2', 'country_code': 'uk'})]
        self.settings = Settings(self.options['exporter_options'])

    def test_transform_empty_batch(self):
        transform = JQTransform({'options': {'jq_filter': '.'}}, self.settings)
        self.assertTrue(list(transform.transform_batch([])) == [])

    def test_no_transform_batch(self):
        transform = JQTransform({'options': {'jq_filter': '.'}}, self.settings)
        index = 0
        for item in list(transform.transform_batch(self.batch)):
            self.assertEqual(item, self.batch[index])
            index += 1

    def test_transform_batch(self):
        transform = JQTransform({'options': {'jq_filter': '{country_code: .country_code}'}}, self.settings)
        for item in list(transform.transform_batch(self.batch)):
            self.assertIn('country_code', item)
            self.assertNotIn('name', item)


class PythonexpTransformTest(unittest.TestCase):
    def setUp(self):
        self.options = {
            'exporter_options': {
                'loglevel': 'DEBUG',
                'loggername': 'export-pipeline'
            },
        }

        self.batch = [BaseRecord({'name': 'item1', 'country_code': 'es'}), BaseRecord({'name': 'item2', 'country_code': 'uk'})]
        self.settings = Settings(self.options['exporter_options'])
        self.transform = PythonexpTransform({'options': {'python_expressions': [
            "item.update({'new_field': item.get('country_code')+'-'+item.get('name')})"]}},
                                            self.settings)

    def test_transform_empty_batch(self):
        self.assertTrue(list(self.transform.transform_batch([])) == [])

    def test_transform_batch(self):
        for item in list(self.transform.transform_batch(self.batch)):
            self.assertIn('country_code', item)
            self.assertIn('name', item)
            self.assertIn('new_field', item)
            self.assertTrue(item['new_field'] == item['country_code']+'-'+item['name'])

