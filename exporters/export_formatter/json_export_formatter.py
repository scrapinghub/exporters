import json
from exporters.export_formatter.base_export_formatter import BaseExportFormatter


class JsonExportFormatter(BaseExportFormatter):

    format_name = 'csv'

    supported_options = {
        'show_titles': {'type': bool, 'default': False}
    }

    def __init__(self, options):
        super(JsonExportFormatter, self).__init__(options)
        self.pretty_print = self.read_option('pretty_print')

    def format(self, batch):
        for item in batch:
            if self.pretty_print:
                item.formatted = self._format(item)
            else:
                item.formatted = json.dumps(item)
            yield item

    def _format(self, item):
        return json.dumps(item, indent=2, sort_keys=True)
