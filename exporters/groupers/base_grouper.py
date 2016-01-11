from exporters.pipeline.base_pipeline_item import BasePipelineItem
from exporters.logger.base_logger import FilterLogger


class BaseGrouper(BasePipelineItem):
    """
    This module adds support to grouping items. It must implement the following methods:

    - group_batch(batch)
        It adds grouping info to all the items from a batch. Every item, which is a BaseRecord,
        has a group_membership attribute that should be updated by this method before yielding it.
    """

    def __init__(self, configuration):
        super(BaseGrouper, self).__init__(configuration)
        self.logger = FilterLogger(
            {'log_level': configuration.get('log_level'), 'logger_name': configuration.get('logger_name')})

    def group_batch(self, batch):
        """
        Yields items with group_membership attribute filled
        """
        raise NotImplementedError
