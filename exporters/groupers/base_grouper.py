from exporters.pipeline.base_pipeline_item import BasePipelineItem
from exporters.logger.base_logger import FilterLogger


class BaseGrouper(BasePipelineItem):
    """
    Base class fro groupers
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
