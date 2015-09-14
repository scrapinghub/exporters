from exporters.logger.base_logger import StatsManagerLogger
from exporters.pipeline.base_pipeline_item import BasePipelineItem


class BaseStatsManager(BasePipelineItem):

    def __init__(self, options, settings):
        super(BaseStatsManager, self).__init__(options, settings)
        self.settings = settings
        self.check_options()
        self.stats = {}
        self.logger = StatsManagerLogger(self.settings)

    def populate(self):
        raise NotImplementedError
