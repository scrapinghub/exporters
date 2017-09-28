from __future__ import absolute_import
import unittest

from exporters.records.base_record import BaseRecord
from exporters.transform.pythonmap import PythonMapTransform


def create_map_transform(map_expr):
    return PythonMapTransform({'options': {'map': map_expr}})


class PythonMapTransformTest(unittest.TestCase):

    def setUp(self):
        self.options = {
            'exporter_options': {
                'log_level': 'DEBUG',
                'logger_name': 'export-pipeline'
            },
        }

    def sample_batch(self):
        return [
            BaseRecord({'name': 'item1', 'country': 'es'}),
            BaseRecord({'name': 'item2', 'country': 'uk'}),
        ]

    def test_transform_empty_batch(self):
        transform = create_map_transform('""')
        self.assertEqual(list(transform.transform_batch([])), [])

    def test_transform_static(self):
        # given:
        batch = self.sample_batch()
        transform = create_map_transform('""')
        # when:
        result = list(transform.transform_batch(batch))
        # then:
        self.assertEqual(result, ["", ""])

    def test_transform_invalid_code(self):
        with self.assertRaises(SyntaxError):
            create_map_transform("{")

    def test_transform_dynamic(self):
        # given:
        batch = self.sample_batch()
        transform = create_map_transform('{"name": item.get("name") + " blah"}')
        # when:
        result = list(transform.transform_batch(batch))
        # then:
        expected = [{"name": "item1 blah"}, {"name": "item2 blah"}]
        self.assertEqual(result, expected)

    def test_transform_augmenting_item(self):
        # given:
        batch = self.sample_batch()
        expr = 'dict(item.items() + dict(upper_name=item["name"].upper()).items())'
        transform = create_map_transform(expr)
        # when:
        result = list(transform.transform_batch(batch))
        # then:
        expected = [
            {"name": "item1", "country": "es", "upper_name": "ITEM1"},
            {"name": "item2", "country": "uk", "upper_name": "ITEM2"},
        ]
        self.assertEqual(result, expected)

    def test_transform_using_stdlib_functions(self):
        # given:
        batch = self.sample_batch()
        transform = create_map_transform('tuple(sorted(item.values()))')
        # when:
        result = list(transform.transform_batch(batch))
        # then:
        self.assertEqual(result, [('es', 'item1'), ('item2', 'uk')])

    def test_transform_allows_regexp(self):
        # given:
        batch = self.sample_batch()
        transform = create_map_transform('bool(re.match("u.", item.get("country")))')
        # when:
        result = list(transform.transform_batch(batch))
        # then:
        self.assertEqual(result, [False, True])
