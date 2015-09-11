from exporters.groupers.base_grouper import BaseGrouper


class NoGrouper(BaseGrouper):
    """
    Default group module, used when no grouping strategies are needed.
    """
    parameters = {

    }

    def __init__(self, options, settings):
        super(NoGrouper, self).__init__(options, settings)

    def group_batch(self, batch):
        return batch
