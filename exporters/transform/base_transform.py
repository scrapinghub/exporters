from exporters.logger.base_logger import TransformLogger
from exporters.pipeline.base_pipeline_item import BasePipelineItem


class BaseTransform(BasePipelineItem):
    """
    This module receives a batch and writes it where needed. It can implement the following methods:
    """

    def __init__(self, configuration, settings):
        super(BaseTransform, self).__init__(configuration, settings)
        self.logger = TransformLogger(self.settings)

    def transform_batch(self, batch):
        """
        Receives the batch, transforms it, and returns it.
        """
        raise NotImplementedError
