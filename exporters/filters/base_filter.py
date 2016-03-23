from exporters.logger.base_logger import FilterLogger
from exporters.pipeline.base_pipeline_item import BasePipelineItem


class BaseFilter(BasePipelineItem):
    """
    This module receives a batch, filter it according to some parameters, and returns it.
    """
    log_at_every = 1000

    def __init__(self, options, metadata):
        super(BaseFilter, self).__init__(options, metadata)
        self.check_options()
        self.logger = FilterLogger(
            {'log_level': options.get('log_level'), 'logger_name': options.get('logger_name')})
        self.set_metadata('filtered_out', 0)
        self.total = 0

    def _log_progress(self):
        if self.total % self.log_at_every == 0:
            self.logger.info('Filtered out %d records from %d total' %
                             (self.get_metadata('filtered_out'), self.total))

    def filter_batch(self, batch):
        """
        Receives the batch, filters it, and returns it.
        """
        for item in batch:
            if self.filter(item):
                yield item
            else:
                self.set_metadata('filtered_out',
                                  self.get_metadata('filtered_out') + 1)

            self.total += 1
            self._log_progress()

    def filter(self, item):
        """
        It receives an item and returns True if the filter must be included, or False if not
        """
        raise NotImplementedError

    def set_metadata(self, key, value, module='filter'):
        super(BaseFilter, self).set_metadata(key, value, module)

    def update_metadata(self, data, module='filter'):
        super(BaseFilter, self).update_metadata(data, module)

    def get_metadata(self, key, module='filter'):
        return super(BaseFilter, self).get_metadata(key, module)

    def get_all_metadata(self, module='filter'):
        return super(BaseFilter, self).get_all_metadata(module)
