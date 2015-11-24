import unittest

from exporters.writers.reduce_writer import ReduceWriter
from exporters.records.base_record import BaseRecord


class ReduceWriterTest(unittest.TestCase):
    def test_should_reduce_items(self):
        batch = [
            BaseRecord({'name': 'item1', 'country_code': 'es'}),
            BaseRecord({'name': 'item2', 'country_code': 'uk'}),
        ]

        reduce_code = """
def reduce_function(item, accumulator=None):
    from collections import Counter
    if accumulator is None:
        accumulator = Counter()
    for key in item:
        accumulator[key] += 1
    return dict(accumulator)
"""
        writer = ReduceWriter({"options": {"code": reduce_code}})
        self.assertEquals({'country_code': 1, 'name': 1}, writer.reduce_function(batch[0]))
