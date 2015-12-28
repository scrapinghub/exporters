from exporters.stats_managers.base_stats_manager import BaseStatsManager


class BasicStatsManager(BaseStatsManager):

    def iteration_report(self, times, stats):
        pass

    def final_report(self, stats):
        self.logger.debug(repr(stats))
