import gzip
import os
import shutil
import tempfile
import uuid
import errno
from exporters.pipeline.base_pipeline_item import BasePipelineItem


class BaseExportFormatter(BasePipelineItem):

    file_extension = None

    def set_grouping_info(self, grouping_info):
        self.grouping_info = grouping_info

    def export_item(self, item):
        raise NotImplementedError

    def start_exporting(self, key):
        pass

    def finish_exporting(self, key):
        pass
