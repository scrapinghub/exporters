from exporters.groupers.base_grouper import BaseGrouper


class NoGrouper(BaseGrouper):
    """
    Default group module, used when no grouping strategies are needed.
    """

    def __init__(self, *args, **kwargs):
        super(NoGrouper, self).__init__(*args, **kwargs)

    def group_batch(self, batch):
        return batch
