from exporters.pipeline.base_pipeline_item import BasePipelineItem
from exporters.logger.base_logger import FilterLogger


class BaseGrouper(BasePipelineItem):
    """
    This module adds support to grouping items. It must implement the following methods:
    """

    def __init__(self, configuration):
        super(BaseGrouper, self).__init__(configuration)
        self.logger = FilterLogger(
            {'log_level': configuration.get('log_level'), 'logger_name': configuration.get('logger_name')})

    def group_batch(self, batch):
        """
        Returns the grouped batch.
        """
        raise NotImplementedError
