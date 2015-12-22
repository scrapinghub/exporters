import json
from collections import OrderedDict
from exporters.stats_managers.basic_stats_manager import BasicStatsManager


class LoggingStatsManager(BasicStatsManager):

    def iteration_times(self, times):
        prev = times['started']
        times.pop('started')
        data = OrderedDict()
        for field, value in times.iteritems():
            data[field] = (value - prev).total_seconds()
            prev = value
        self.logger.info(json.dumps(data))
