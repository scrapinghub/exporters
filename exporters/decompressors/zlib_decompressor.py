import zlib
from exporters.decompressors.base_decompressor import BaseDecompressor


def create_decompressor():
    # create zlib decompressor enabling automatic header detection:
    # See: http://stackoverflow.com/a/22310760/149872
    AUTOMATIC_HEADER_DETECTION_MASK = 32
    return zlib.decompressobj(AUTOMATIC_HEADER_DETECTION_MASK | zlib.MAX_WBITS)


class ZLibDecompressor(BaseDecompressor):
    def decompress(self, stream):
        dec = create_decompressor()
        for chunk in stream:
            rv = dec.decompress(chunk)
            if rv:
                yield rv
            if dec.unused_data:
                stream.unshift(dec.unused_data)
                dec = create_decompressor()
