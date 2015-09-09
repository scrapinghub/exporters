from exporters.logger.base_logger import TransformLogger
from exporters.pipeline.base_pipeline_item import BasePipelineItem


class BaseTransform(BasePipelineItem):
    """
    This module receives a batch and writes it where needed. It can implement the following methods:
    """
    def __init__(self, options, settings):
        super(BaseTransform, self).__init__(options, settings)
        self.settings = settings
        self.requirements = getattr(self, 'requirements', {})
        self.check_options()
        self.logger = TransformLogger(self.settings)

    def transform_batch(self, batch):
        """
        Receives the batch, transforms it, and returns it.
        """
        raise NotImplementedError
