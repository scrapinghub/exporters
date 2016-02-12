import dicttoxml
from exporters.export_formatter.base_export_formatter import BaseExportFormatter


class XMLExportFormatter(BaseExportFormatter):
    """
    This export formatter provides a way of exporting items in JSON format. This one is the
    default formatter.

        - pretty_print(bool)
            If set to True, items will be exported with an ident of 2 and keys sorted, they
            will exported with a text line otherwise.
    """
    format_name = 'xml'

    supported_options = {
    }

    def __init__(self, options):
        super(XMLExportFormatter, self).__init__(options)

    def format(self, batch):
        for item in batch:
            item.formatted = '<item>{}</item>'.format(dicttoxml.dicttoxml(item, root=False))
            item.format = self.format_name
            yield item
