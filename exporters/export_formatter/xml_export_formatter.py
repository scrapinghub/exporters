import dicttoxml
from exporters.export_formatter.base_export_formatter import BaseExportFormatter
import collections


class XMLExportFormatter(BaseExportFormatter):
    """
    This export formatter provides a way of exporting items in XML format

    """
    format_name = 'xml'

    supported_options = {
    }

    def __init__(self, options):
        super(XMLExportFormatter, self).__init__(options)

    def format(self, batch):
        for item in batch:
            item.formatted = '<item>{}</item>'.format(
                dicttoxml.dicttoxml(collections.OrderedDict(item), root=False))
            item.format = self.format_name
            yield item
