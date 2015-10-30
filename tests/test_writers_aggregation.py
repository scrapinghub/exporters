import re
import unittest

import boto
import moto
import mock

from exporters.records.base_record import BaseRecord
from exporters.writers.aggregation_stats_writer import AggregationStatsWriter
from exporters.writers.s3_writer import S3Writer


class AggregationStatsWriterTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_write_aggregated(self):
        # given
        data = [
            {'name': 'Roberto', 'birthday': '12/05/1987'},
            {'name': 'Claudia', 'birthday': '21/12/1985'},
            {'name': 'Bob', 'birthday': '21/12/1985'},
            {'name': 'Claude', 'last_login': '21/12/1985'},
        ]
        items_to_write = [BaseRecord(d) for d in data]
        options = self.get_writer_config()

        # when:
        writer = AggregationStatsWriter(options)
        writer.write_batch(items_to_write)
        writer.close_writer()

        # then:
        expected_info = {'birthday': {'ocurrences': 3, 'coverage': 75.0},
                         'last_login': {'ocurrences': 1, 'coverage': 25.0},
                         'name': {'ocurrences': 4, 'coverage': 100.0}}
        self.assertEqual(expected_info, writer._get_aggregated_info())

    def get_writer_config(self):
        return {
            'name': 'exporters.writers.aggregation_stats_writer.AggregationStatsWriter',
            'options': {

            }
        }
