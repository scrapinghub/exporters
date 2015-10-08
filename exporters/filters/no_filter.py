from exporters.filters.base_filter import BaseFilter


class NoFilter(BaseFilter):
    """
    It leaves the batch as is. This is provided for the cases where no filters are needed on the original items.
    """

    def __init__(self, options):
        super(NoFilter, self).__init__(options)

    def filter_batch(self, batch):
        return batch
