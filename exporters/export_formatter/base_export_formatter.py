from exporters.pipeline.base_pipeline_item import BasePipelineItem


class BaseExportFormatter(BasePipelineItem):

    file_extension = None

    def export_item(self, item):
        raise NotImplementedError

    def start_exporting(self):
        pass

    def finish_exporting(self):
        pass
