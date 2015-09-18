from exporters.logger.base_logger import FilterLogger
from exporters.pipeline.base_pipeline_item import BasePipelineItem


class BaseFilter(BasePipelineItem):
    log_at_every = 1000
    """
    This module receives a batch, filter it according to some parameters, and returns it.
    It must implement the following methods:
    """
    def __init__(self, options):
        super(BaseFilter, self).__init__(options)
        self.check_options()
        self.logger = FilterLogger(options.get('settings', {}))
        self.filtered_out = 0
        self.total = 0

    def _log_progress(self):
        if self.total % self.log_at_every == 0:
            self.logger.info('Filtered out %d records from %d total' %
                             (self.filtered_out, self.total))

    def filter_batch(self, batch):
        """
        Receives the batch, filters it, and returns it.
        """
        for item in batch:
            if self.filter(item):
                yield item
            else:
                self.filtered_out += 1

            self.total += 1
            self._log_progress()

    def filter(self, item):
        """
        Return True if item should be included.
        """
        raise NotImplementedError
