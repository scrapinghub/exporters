import os
from sh_scrapy.stats import HubStorageStatsCollector
from exporters.stats_managers.base_stats_manager import BaseStatsManager


def _get_fake_crawler():
    Crawler = type('Crawler', (), {'settings': type('Settings', (), {})()})
    Crawler.settings.getbool = lambda x: True
    return Crawler()

class DashStatsManager(BaseStatsManager):
    def __init__(self, options, settings):
        super(DashStatsManager, self).__init__(options, settings)
        if 'SHUB_JOBAUTH' in os.environ:
            self.hs_stats = HubStorageStatsCollector(_get_fake_crawler())
        else:
            self.hs_stats = None
            self.logger.error('DashStatsManager can only be used in dash projects')

    def populate(self):
        if self.hs_stats:
            self.hs_stats._stats = self.stats
            self.hs_stats._upload_stats()
            self.logger.debug('Uploaded stats to dash')
        else:
            self.logger.debug(str(self.stats))
