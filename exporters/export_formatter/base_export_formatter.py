import uuid

import datetime

from exporters.pipeline.base_pipeline_item import BasePipelineItem


class BaseExportFormatter(BasePipelineItem):

    file_extension = None

    supported_options = {
        'base_filename': {'type': basestring, 'default': None}
    }

    def __init__(self, options):
        super(BaseExportFormatter, self).__init__(options)
        self.file_count = 0
        self.date = datetime.datetime.now()
        self.base_filename = self.read_option('base_filename')

    def format(self, item):
        raise NotImplementedError

    def format_header(self):
        return ''

    def format_footer(self):
        return ''

    def new_filename(self):
        if not self.base_filename:
            return uuid.uuid4()

        filename = '{}{:04d}'.format(self.base_filename, self.file_count)
        self.file_count += 1
        return self.date.strftime(filename)
