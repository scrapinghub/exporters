from exporters.logger.base_logger import StatsManagerLogger
from exporters.pipeline.base_pipeline_item import BasePipelineItem


class BaseStatsManager(BasePipelineItem):

    def __init__(self, configuration, settings):
        super(BaseStatsManager, self).__init__(configuration, settings)
        self.stats = {}
        self.logger = StatsManagerLogger(self.settings)

    def populate(self):
        raise NotImplementedError
