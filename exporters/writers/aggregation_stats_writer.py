from collections import Counter
from exporters.writers.base_writer import BaseWriter, ItemsLimitReached


class AggregationStatsWriter(BaseWriter):
    """
    This writer keeps track of keys occurences in dataset items. It provides information
    about the number and percentage of every possible key in a dataset.
    """

    def __init__(self, options):
        super(AggregationStatsWriter, self).__init__(options)
        self.aggregated_info = {'occurrences': Counter()}
        self.logger.info('AggregationStatsWriter has been initiated')

    def write_batch(self, batch):
        for item in batch:
            for key in item:
                self.aggregated_info['occurrences'][key] += 1
            self._increment_written_items()
            if self.items_limit and self.items_limit == self.stats['items_count']:
                raise ItemsLimitReached('Finishing job after items_limit reached: {} items written.'.format(self.stats['items_count']))
        self.logger.debug('Wrote items')

    def _get_aggregated_info(self):
        agg_results = {}
        for key in self.aggregated_info['occurrences']:
            agg_results[key] = {
                'occurrences': self.aggregated_info['occurrences'].get(key),
                'coverage': (float(self.aggregated_info['occurrences'].get(key))/float(self.stats['items_count']))*100
            }
        return agg_results

    def close(self):
        agg_results = self._get_aggregated_info()
        self.logger.info('---------------------')
        self.logger.info('DATASET KEYS COVERAGE')
        self.logger.info('---------------------')
        self.logger.info(repr(agg_results))
        self.logger.info('---------------------')
        super(AggregationStatsWriter, self).close()
