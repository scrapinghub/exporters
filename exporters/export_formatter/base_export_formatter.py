from exporters.pipeline.base_pipeline_item import BasePipelineItem


class BaseExportFormatter(BasePipelineItem):
    """
    This is the base formatter class. All formatter must inherit from here
    """

    def format(self, batch):
        raise NotImplementedError
