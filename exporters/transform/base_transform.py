from exporters.logger.base_logger import TransformLogger
from exporters.pipeline.base_pipeline_item import BasePipelineItem


class BaseTransform(BasePipelineItem):
    """
    This module receives a batch and writes it where needed. It can implement the following methods:
    """

    def __init__(self, options, metadata=None):
        super(BaseTransform, self).__init__(options, metadata)
        self.logger = TransformLogger({
            'log_level': options.get('log_level'),
            'logger_name': options.get('logger_name')
        })

    def transform_batch(self, batch):
        """
        Receives the batch, transforms it, and returns it.
        """
        raise NotImplementedError

    def set_metadata(self, key, value, module='transform'):
        super(BaseTransform, self).set_metadata(key, value, module)

    def update_metadata(self, data, module='transform'):
        super(BaseTransform, self).update_metadata(data, module)

    def get_metadata(self, key, module='transform'):
        return super(BaseTransform, self).get_metadata(key, module)

    def get_all_metadata(self, module='transform'):
        return super(BaseTransform, self).get_all_metadata(module)
