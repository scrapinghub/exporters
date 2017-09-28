from __future__ import absolute_import
import unittest
from exporters.records.base_record import BaseRecord
from exporters.transform.jq_transform import JQTransform


class JqTransformTest(unittest.TestCase):
    def setUp(self):
        self.options = {
            'exporter_options': {
                'log_level': 'DEBUG',
                'logger_name': 'export-pipeline'
            },
        }

        self.batch = [
            BaseRecord({'name': 'item1', 'country_code': 'es'}),
            BaseRecord({'name': 'item2', 'country_code': 'uk'}),
        ]

    def test_transform_empty_batch(self):
        transform = JQTransform({'options': {'jq_filter': '.'}})
        self.assertEquals([], list(transform.transform_batch([])))

    def test_no_transform_batch(self):
        transform = JQTransform({'options': {'jq_filter': '.'}})
        self.assertEqual(self.batch, list(transform.transform_batch(self.batch)))

    def test_transform_batch(self):
        transform = JQTransform({'options': {'jq_filter': '{country: .country_code}'}})
        expected = [{'country': 'es'}, {'country': 'uk'}]
        self.assertEqual(expected, list(transform.transform_batch(self.batch)))

    def test_transform_with_filter(self):
        for country in ['es', 'uk']:
            jq_filter = 'select(.country_code == "%s") | {country_code}' % country
            transform = JQTransform({'options': {'jq_filter': jq_filter}})
            expected = [{'country_code': country}]
            self.assertEqual(expected, list(transform.transform_batch(self.batch)))

    def test_invalid_jq_expression(self):
        with self.assertRaisesRegexp(ValueError, "jq: 1 compile error"):
            JQTransform({'options': {'jq_filter': 'blah'}})
