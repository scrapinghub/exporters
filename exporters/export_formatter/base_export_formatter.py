from exporters.pipeline.base_pipeline_item import BasePipelineItem


class BaseExportFormatter(BasePipelineItem):

    file_extension = None
    item_separator = '\n'

    def __init__(self, options, metadata=None):
        super(BaseExportFormatter, self).__init__(options, metadata)

    def format(self, item):
        raise NotImplementedError

    def format_header(self):
        return ''

    def format_footer(self):
        return ''
