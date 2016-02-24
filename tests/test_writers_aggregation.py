import unittest

from exporters.export_formatter.json_export_formatter import JsonExportFormatter
from exporters.records.base_record import BaseRecord
from exporters.writers.aggregation_stats_writer import AggregationStatsWriter


class AggregationStatsWriterTest(unittest.TestCase):

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
        writer = AggregationStatsWriter(options, export_formatter=JsonExportFormatter(dict()))
        writer.write_batch(items_to_write)
        writer.close()

        # then:
        expected_info = {'birthday': {'occurrences': 3, 'coverage': 75.0},
                         'last_login': {'occurrences': 1, 'coverage': 25.0},
                         'name': {'occurrences': 4, 'coverage': 100.0}}
        self.assertEqual(expected_info, writer._get_aggregated_info())

    def get_writer_config(self):
        return {
            'name': 'exporters.writers.aggregation_stats_writer.AggregationStatsWriter',
            'options': {

            }
        }
