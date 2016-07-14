from exporters.pipeline.base_pipeline_item import BasePipelineItem


class BaseDecompressor(BasePipelineItem):
    def decompress(self):
        raise NotImplementedError()
