import dicttoxml
from exporters.export_formatter.base_export_formatter import BaseExportFormatter
import collections


class XMLExportFormatter(BaseExportFormatter):
    """
    This export formatter provides a way of exporting items in XML format

    """
    file_extension = 'xml'

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

    def start_exporting(self, key):
        path = self.create_new_buffer_file(key)
        with open(path, 'a') as f:
            f.write('<{}>\n'.format(self.root_name))
        return path

    def finish_exporting(self, key):
        path = self.get_group_path(key)
        with open(path, 'a') as f:
            f.write('</{}>'.format(self.root_name))
        return path

    def export_item(self, item):
        fields_len = len(self.fields_order)
        ordered_item = collections.OrderedDict(
            sorted(item.items(),
                   key=lambda kv: self.fields_order.get(kv[0], fields_len))
        )
        return '<{0}>{1}</{0}>'.format(self.item_name, dicttoxml.dicttoxml(ordered_item, root=False,
                                                                           attr_type=self.attr_type))
