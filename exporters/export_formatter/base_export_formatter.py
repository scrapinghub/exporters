from exporters.pipeline.base_pipeline_item import BasePipelineItem


class BaseExportFormatter(BasePipelineItem):

    file_extension = None

    def format(self, item):
        raise NotImplementedError

    def format_header(self):
        return ''

    def format_footer(self):
        return ''
