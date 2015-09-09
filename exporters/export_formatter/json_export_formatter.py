import json
from exporters.export_formatter.base_export_formatter import BaseExportFormatter


class JsonExportFormatter(BaseExportFormatter):
    def __init__(self, options, settings):
        super(JsonExportFormatter, self).__init__(options, settings)

    def format(self, batch):
        for item in batch:
            item.formatted = json.dumps(item)
            yield item
