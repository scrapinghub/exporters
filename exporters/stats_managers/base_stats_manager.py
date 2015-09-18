from exporters.logger.base_logger import StatsManagerLogger
from exporters.pipeline.base_pipeline_item import BasePipelineItem


class BaseStatsManager(BasePipelineItem):

    def __init__(self, configuration):
        super(BaseStatsManager, self).__init__(configuration)
        self.stats = {}
        self.logger = StatsManagerLogger(configuration.get('settings', {}))

    def populate(self):
        raise NotImplementedError
