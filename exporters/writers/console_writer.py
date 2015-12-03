import json

from exporters.writers.base_writer import BaseWriter, ItemsLimitReached


class ConsoleWriter(BaseWriter):
    """
    It is just a writer with testing purposes. It prints every item in console.
    """

    def __init__(self, options):
        super(ConsoleWriter, self).__init__(options)
        self.logger.info('ConsoleWriter has been initiated')
        self.pretty_print = self.options.get('pretty_print', False)

    def write_batch(self, batch):
        for item in batch:
            formatted_item = item.formatted

            if self.pretty_print:
                formatted_item = self._format(formatted_item)

            print formatted_item
            self._increment_written_items()
            if self.items_limit and self.items_limit == self.stats['items_count']:
                raise ItemsLimitReached('Finishing job after items_limit reached: {} items written.'.format(self.stats['items_count']))
        self.logger.debug('Wrote items')

    def _format(self, item):
        try:
            return json.dumps(json.loads(item), indent=2)
        except:
            return item
