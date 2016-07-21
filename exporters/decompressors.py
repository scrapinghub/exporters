from exporters.pipeline.base_pipeline_item import BasePipelineItem
import zlib

__all__ = ['BaseDecompressor', 'ZLibDecompressor', 'NoDecompressor']


class BaseDecompressor(BasePipelineItem):
    def decompress(self):
        raise NotImplementedError()


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


class NoDecompressor(BaseDecompressor):
    def decompress(self, stream):
        return stream  # Input already uncompressed
