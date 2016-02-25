import unittest

from exporters.export_formatter.json_export_formatter import JsonExportFormatter
from exporters.writers.reduce_writer import ReduceWriter
from exporters.records.base_record import BaseRecord


class ReduceWriterTest(unittest.TestCase):
    def test_should_reduce_items(self):
        batch = [
            BaseRecord({'name': 'item1', 'country_code': 'es'}),
            BaseRecord({'name': 'item2', 'country_code': 'uk'}),
            BaseRecord({'name': 'item3', 'something': 'else'}),
        ]

        reduce_code = """
def reduce_function(item, accumulator=None):
    from collections import Counter
    accumulator = accumulator or Counter()
    for key in item:
        accumulator[key] += 1
    return accumulator
"""
        writer = ReduceWriter({"options": {"code": reduce_code}}, export_formatter=JsonExportFormatter(dict()))
        writer.write_batch(batch)
        writer.write_batch(batch)
        expected = {'country_code': 4, 'name': 6, 'something': 2}
        self.assertEquals(expected, dict(writer.reduced_result))
        writer.close()
