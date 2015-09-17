from exporters.filters.base_filter import BaseFilter


class NoFilter(BaseFilter):
    """
    It leaves the batch as is. This is provided for the cases where no filters are needed on the original items.
    """
    # List of options
    parameters = {}

    def __init__(self, options):
        super(NoFilter, self).__init__(options)
        self.logger.info('NoFilter has been initiated')

    def filter_batch(self, batch):
        return batch
