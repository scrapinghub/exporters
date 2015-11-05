from exporters.logger.base_logger import StatsManagerLogger
from exporters.pipeline.base_pipeline_item import BasePipelineItem


class BaseStatsManager(BasePipelineItem):
    def __init__(self, options):
        super(BaseStatsManager, self).__init__(options)
        self.logger = StatsManagerLogger({'log_level': options.get('log_level'),
                                          'logger_name': options.get('logger_name')})

    def populate(self):
        raise NotImplementedError
