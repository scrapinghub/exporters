from exporters.decompressors.base import BaseDecompressor


class NoCompression(BaseDecompressor):
    def decompress(self, stream):
        return stream  # Input already uncompressed
