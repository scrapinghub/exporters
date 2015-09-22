from exporters.groupers.base_grouper import BaseGrouper


class NoGrouper(BaseGrouper):
    """
    Default group module, used when no grouping strategies are needed.
    """

    def __init__(self, options):
        super(NoGrouper, self).__init__(options)

    def group_batch(self, batch):
        return batch
