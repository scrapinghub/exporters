from __future__ import absolute_import
import unittest
from exporters.deserializers import CSVDeserializer
from exporters.iterio import IterIO
import random
from six.moves import range


def randbytes(howmany):
    return "".join([chr(random.randint(0, 255)) for i in range(howmany)])


class DecompressorsTest(unittest.TestCase):
    def test_deserializer(self):
        deserializer = CSVDeserializer({}, None)
        with open('tests/data/dummy_data.csv', 'r') as f:
            items = list(deserializer.deserialize(IterIO(f)))

        expected_items = [
            {'bar': 'hello', 'baz': 'world', 'id': '1'},
            {'bar': 'foo', 'baz': 'bar', 'id': '2'},
            {'bar': 'xdxd', 'baz': 'xdxd', 'id': '3'}
        ]
        assert items == expected_items
