import unittest
import zlib
from exporters.decompressors import ZLibDecompressor, NoDecompressor
from exporters.iterio import IterIO
from io import BytesIO
import random


def randbytes(howmany):
    return "".join([chr(random.randint(0, 255)) for i in range(howmany)])


class DecompressorsTest(unittest.TestCase):
    def test_zlib_decompressor(self):
        decompressor = ZLibDecompressor({}, None)
        compressed = IterIO(BytesIO(zlib.compress('helloworld')))
        assert IterIO(decompressor.decompress(compressed)).read() == 'helloworld'

        # Mutiple headers
        decompressor = ZLibDecompressor({}, None)
        compressed = zlib.compress('hello') + zlib.compress('world') + zlib.compress('foobar')
        compressed = IterIO(BytesIO(compressed))
        assert IterIO(decompressor.decompress(compressed)).read() == 'helloworldfoobar'

        # Parts are more than one chunk
        decompressor = ZLibDecompressor({}, None)
        parts = [randbytes(2**10), "hello", "world", randbytes(2**11)]
        compressed = "".join(zlib.compress(part) for part in parts)
        compressed = IterIO(BytesIO(compressed))
        assert IterIO(decompressor.decompress(compressed)).read() == "".join(parts)

    def test_no_compression(self):
        decompressor = NoDecompressor({}, None)
        compressed = IterIO(BytesIO('helloworld'))
        assert IterIO(decompressor.decompress(compressed)).read() == 'helloworld'
