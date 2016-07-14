from exporters.decompressors.base_decompressor import BaseDecompressor


class NoCompression(BaseDecompressor):
    def decompress(self, stream):
        return stream  # Input already uncompressed
