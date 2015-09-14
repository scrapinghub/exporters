from exporters.pipeline.base_pipeline_item import BasePipelineItem


class BaseGrouper(BasePipelineItem):
    """
    This module adds support to grouping items. It must implement the following methods:
    """
    def __init__(self, options, settings):
        super(BaseGrouper, self).__init__(options, settings)
        self.settings = settings
        self.check_options()

    def group_batch(self, batch):
        """
        Returns the grouped batch.
        """
        raise NotImplementedError
