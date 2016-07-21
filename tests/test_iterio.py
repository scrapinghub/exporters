import unittest
from exporters.iterio import IterIO


class IterIOTest(unittest.TestCase):
    def test_read_all(self):
        io = IterIO(iter(['hello', 'world']))
        assert io.read() == 'helloworld'
        assert io.read() == ''
        assert io.tell() == len('helloworld')

    def test_read_bytes(self):
        io = IterIO(iter(['hello', 'world']))
        for i, c in enumerate('helloworld'):
            assert io.tell() == i
            assert io.read(1) == c
        assert io.read(1) == ''
        assert io.tell() == len('helloworld')

    def test_read_length(self):
        io = IterIO(iter(['hello', 'world']))
        for c in 'hel', 'low', 'orl', 'd':
            assert io.read(3) == c
        assert io.read(1) == ''
        assert io.tell() == len('helloworld')
        io = IterIO(iter(['hello', 'world']))
        for c in 'hell', 'owor', 'ld':
            assert io.read(4) == c
        assert io.read(1) == ''
        assert io.tell() == len('helloworld')

    def test_read_lines(self):
        io = IterIO(iter(['he\n\nllo', '\nworl\nd']))
        assert io.readlines() == ['he\n', '\n', 'llo\n', 'worl\n', 'd']

    def test_line_mode(self):
        io = IterIO(iter(['he\n\nllo', '\nworl\nd']), mode="lines")
        assert list(io) == ['he\n', '\n', 'llo\n', 'worl\n', 'd']
