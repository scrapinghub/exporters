from exporters.pipeline.base_pipeline_item import BasePipelineItem


class BaseDeserializer(BasePipelineItem):
    def deserialize(self, stream):
        raise NotImplementedError()
