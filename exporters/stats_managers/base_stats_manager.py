from exporters.logger.base_logger import StatsManagerLogger
from exporters.pipeline.base_pipeline_item import BasePipelineItem


class BaseStatsManager(BasePipelineItem):
    """
    Base class for stats managers
    """

    def __init__(self, *args, **kwargs):
        super(BaseStatsManager, self).__init__(*args, **kwargs)
        self.logger = StatsManagerLogger({
            'log_level': self.read_option('log_level'),
            'logger_name': self.read_option('logger_name')
        })

    def iteration_report(self, times):
        raise NotImplementedError

    def final_report(self):
        raise NotImplementedError

    def set_metadata(self, key, value, module='stats'):
        super(BaseStatsManager, self).set_metadata(key, value, module)

    def update_metadata(self, data, module='stats'):
        super(BaseStatsManager, self).update_metadata(data, module)

    def get_metadata(self, key, module='stats'):
        return super(BaseStatsManager, self).get_metadata(key, module)

    def get_all_metadata(self, module='stats'):
        return super(BaseStatsManager, self).get_all_metadata(module)
