import logging
from exporters.export_formatter.base_export_formatter import BaseExportFormatter
from exporters.utils import str_list
import collections


DEFAULT_XML_HEADER = '<?xml version="1.0" encoding="UTF-8"?>'


class XMLExportFormatter(BaseExportFormatter):
    """
    This export formatter provides a way of exporting items in XML format

    """
    file_extension = 'xml'

    supported_options = {
        'attr_type': {'type': bool, 'default': True},
        'fields_order': {'type': str_list, 'default': []},
        'item_name': {'type': basestring, 'default': 'item'},
        'root_name': {'type': basestring, 'default': 'root'},
        'xml_header': {'type': basestring, 'default': DEFAULT_XML_HEADER}
    }

    def __init__(self, *args, **kwargs):
        super(XMLExportFormatter, self).__init__(*args, **kwargs)
        self.attr_type = self.read_option('attr_type')
        self.item_name = self.read_option('item_name')
        self.root_name = self.read_option('root_name')
        self.xml_header = self.read_option('xml_header')
        self.fields_order = self._get_fields()

    def _get_fields(self):
        fields = self.read_option('fields_order')
        return {key: idx for idx, key in enumerate(fields)}

    def format_header(self):
        if self.xml_header:
            return '{}\n<{}>\n'.format(self.xml_header, self.root_name)
        return '<{}>\n'.format(self.root_name)

    def format_footer(self):
        return '\n</{}>'.format(self.root_name)

    def format(self, item):
        import dicttoxml
        dicttoxml.LOG.setLevel(logging.WARNING)
        fields_len = len(self.fields_order)
        ordered_item = collections.OrderedDict(
            sorted(item.items(),
                   key=lambda kv: self.fields_order.get(kv[0], fields_len))
        )
        return '<{0}>{1}</{0}>'.format(
            self.item_name, dicttoxml.dicttoxml(ordered_item, root=False,
                                                attr_type=self.attr_type))
