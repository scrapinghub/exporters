from exporters.pipeline.base_pipeline_item import BasePipelineItem


class BaseExportFormatter(BasePipelineItem):
    def __init__(self, options, settings):
        self.settings = settings
        self.options = options
        self.requirements = getattr(self, 'requirements', {})
        self.check_options()

    def format(self, bath):
        raise NotImplementedError
