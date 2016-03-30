from exporters.writers.base_writer import BaseWriter, ItemsLimitReached


class ConsoleWriter(BaseWriter):
    """
    It is just a writer with testing purposes. It prints every item in console.

    It has no other options.
    """

    def __init__(self, options, *args, **kwargs):
        super(ConsoleWriter, self).__init__(options, *args, **kwargs)
        self.logger.info('ConsoleWriter has been initiated')
        self.pretty_print = self.options.get('pretty_print', False)
        header = self.export_formatter.format_header()
        if header:
            print(header)

    def write_batch(self, batch):
        for item in batch:
            print(self.export_formatter.format(item))
            self.increment_written_items()
            if self.items_limit and self.items_limit == self.get_metadata('items_count'):
                raise ItemsLimitReached('Finishing job after items_limit reached: {} items written.'
                                        .format(self.get_metadata('items_count')))
        self.logger.debug('Wrote items')

    def close(self):
        super(ConsoleWriter, self).close()
        footer = self.export_formatter.format_footer()
        if footer:
            print(footer)
