from exporters.pipeline.base_pipeline_item import BasePipelineItem


class BaseExportFormatter(BasePipelineItem):

    file_extension = None

    def __init__(self, options, metadata=None):
        super(BaseExportFormatter, self).__init__(options, metadata)

    def format(self, item):
        raise NotImplementedError

    def format_header(self):
        return ''

    def format_footer(self):
        return ''

    def set_metadata(self, key, value, module='formatter'):
        super(BaseExportFormatter, self).set_metadata(key, value, module)

    def update_metadata(self, data, module='formatter'):
        super(BaseExportFormatter, self).update_metadata(data, module)

    def get_metadata(self, key, module='formatter'):
        return super(BaseExportFormatter, self).get_metadata(key, module)

    def get_all_metadata(self, module='formatter'):
        return super(BaseExportFormatter, self).get_all_metadata(module)
