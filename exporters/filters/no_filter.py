from exporters.filters.base_filter import BaseFilter


class NoFilter(BaseFilter):
    """
    It leaves the batch as is. This is provided for the cases where no filters are needed
    on the original items.
    """

    def __init__(self, *args, **kwargs):
        super(NoFilter, self).__init__(*args, **kwargs)

    def filter_batch(self, batch):
        return batch
