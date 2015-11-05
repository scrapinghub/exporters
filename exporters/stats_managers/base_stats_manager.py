from exporters.logger.base_logger import StatsManagerLogger
from exporters.pipeline.base_pipeline_item import BasePipelineItem


class BaseStatsManager(BasePipelineItem):
    def __init__(self, options):
        super(BaseStatsManager, self).__init__(options)
        self._init_stats_dict()
        self.logger = StatsManagerLogger({'log_level': options.get('log_level'),
                                          'logger_name': options.get('logger_name')})

    def populate(self):
        raise NotImplementedError

    def _init_stats_dict(self):
        self.stats = {k: {} for k in (
            'reader', 'filter_before', 'filter_after', 'transform', 'persistence',
            'export_formatter', 'grouper', 'writer')}
