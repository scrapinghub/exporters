from exporters.writers.base_writer import BaseWriter, ItemsLimitReached


class AggregationStatsWriter(BaseWriter):
    """
    It is just a writer with testing purposes. It prints every item in console.
    """

    def __init__(self, options):
        super(AggregationStatsWriter, self).__init__(options)
        self.aggregated_info = {}
        self.logger.info('AggregationStatsWriter has been initiated')

    def write_batch(self, batch):
        for item in batch:
            for key in item.keys():
                self.aggregated_info[key] = self.aggregated_info.get(key, 0) + 1
            self.items_count += 1
            if self.items_limit and self.items_limit == self.items_count:
                raise ItemsLimitReached('Finishing job after items_limit reached: {} items written.'.format(self.items_count))
        self.logger.debug('Wrote items')

    def _get_aggregated_info(self):
        agg_results = {}
        for key in self.aggregated_info:
            agg_results[key] = {
                'ocurrences': self.aggregated_info[key],
                'coverage': (float(self.aggregated_info[key])/float(self.items_count))*100
            }
        return agg_results

    def close_writer(self):
        agg_results = self._get_aggregated_info()
        self.logger.info('---------------------')
        self.logger.info('DATASET KEYS COVERAGE')
        self.logger.info('---------------------')
        self.logger.info(repr(agg_results))
        self.logger.info('---------------------')
