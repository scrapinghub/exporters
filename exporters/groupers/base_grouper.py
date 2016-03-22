from exporters.pipeline.base_pipeline_item import BasePipelineItem
from exporters.logger.base_logger import FilterLogger


class BaseGrouper(BasePipelineItem):
    """
    Base class fro groupers
    """

    def __init__(self, options, metadata=None):
        super(BaseGrouper, self).__init__(options, metadata)
        self.logger = FilterLogger({
            'log_level': self.options.get('log_level'),
            'logger_name': self.options.get('logger_name')
        })

    def group_batch(self, batch):
        """
        Yields items with group_membership attribute filled
        """
        raise NotImplementedError

    def set_metadata(self, key, value, module='grouper'):
        super(BaseGrouper, self).set_metadata(key, value, module)

    def update_metadata(self, data, module='grouper'):
        super(BaseGrouper, self).update_metadata(data, module)

    def get_metadata(self, key, module='grouper'):
        return super(BaseGrouper, self).get_metadata(key, module)

    def get_all_metadata(self, module='grouper'):
        return super(BaseGrouper, self).get_all_metadata(module)
