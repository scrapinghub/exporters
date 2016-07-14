from exporters.decompressors.base_decompressor import BaseDecompressor


class NoDecompressor(BaseDecompressor):
    def decompress(self, stream):
        return stream  # Input already uncompressed
