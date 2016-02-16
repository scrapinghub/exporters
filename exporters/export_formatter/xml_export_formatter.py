import dicttoxml
from exporters.export_formatter.base_export_formatter import BaseExportFormatter
import collections

from exporters.records.base_record import BaseRecord


class XMLExportFormatter(BaseExportFormatter):
    """
    This export formatter provides a way of exporting items in XML format

    """
    format_name = 'xml'

    supported_options = {
        'attr_type': {'type': bool, 'default': True},
        'fields_order': {'type': list, 'default': []},
        'item_name': {'type': basestring, 'default': 'item'},
        'root_name': {'type': basestring, 'default': 'root'}
    }

    def __init__(self, options):
        super(XMLExportFormatter, self).__init__(options)
        self.attr_type = self.read_option('attr_type')
        self.item_name = self.read_option('item_name')
        self.root_name = self.read_option('root_name')
        self.fields_order = self._get_fields()

    def _get_fields(self):
        fields = self.read_option('fields_order')
        return {key: idx for idx, key in enumerate(fields)}

    def _write_opening_root(self):
        header = BaseRecord({})
        header.formatted = '<{}>'.format(self.root_name)
        header.format = self.format_name
        header.header = True
        return header

    def _write_closing_root(self):
        bottom = BaseRecord({})
        bottom.formatted = '</{}>'.format(self.root_name)
        bottom.format = self.format_name
        bottom.bottom = True
        return bottom

    def format(self, batch):
        fields_len = len(self.fields_order)

        yield self._write_opening_root()
        yield self._write_closing_root()
        for item in batch:
            ordered_item = collections.OrderedDict(
                sorted(item.items(),
                       key=lambda kv: self.fields_order.get(kv[0], fields_len))
            )
            item.formatted = '<{0}>{1}</{0}>'.format(
                self.item_name,
                dicttoxml.dicttoxml(ordered_item, root=False,
                                    attr_type=self.attr_type))
            item.format = self.format_name
            yield item
