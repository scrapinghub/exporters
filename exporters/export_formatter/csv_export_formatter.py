import csv
import io
import six
from exporters.utils import str_list
from exporters.exceptions import ConfigurationError
from exporters.export_formatter.base_export_formatter import BaseExportFormatter


class CSVExportFormatter(BaseExportFormatter):
    """
    This export formatter provides a way of exporting items in CSV format. This are the
    supported options:

        - show_titles(bool)
            If set to True, first lines of exported files will have a row of column names

        - fields(list)
            List of item fields to be exported

        - schema(dict)
            Valid json schema of dataset items

        - delimiter(str)
            field delimiter character for csv rows
    """
    file_extension = 'csv'

    supported_options = {
        'show_titles': {'type': bool, 'default': True},
        'fields': {'type': str_list, 'default': []},
        'schema': {'type': dict, 'default': {}},
        'delimiter': {'type': basestring, 'default': ','},
    }

    def __init__(self, *args, **kwargs):
        super(CSVExportFormatter, self).__init__(*args, **kwargs)
        self.show_titles = self.read_option('show_titles')
        self.delimiter = self.read_option('delimiter')
        self.fields = self._get_fields()

    def _get_fields_from_schema(self):
        schema = self.read_option('schema')
        return sorted(schema.get('properties', {}).keys())

    def _get_fields(self):
        if self.read_option('fields'):
            return self.read_option('fields')
        elif not self.read_option('schema'):
            raise ConfigurationError('CSV formatter requires at least one of: fields or schema')
        return self._get_fields_from_schema()

    def _encode_string(self, path, key, value):
        if isinstance(value, six.text_type):
            return key, value.encode('utf-8')
        return key, value

    def _create_csv_writer(self, outputf):
        return csv.DictWriter(outputf, fieldnames=self.fields,
                              quoting=csv.QUOTE_NONNUMERIC,
                              delimiter=self.delimiter,
                              extrasaction='ignore')

    def _item_to_csv(self, item):
        from boltons.iterutils import remap
        output = io.BytesIO()
        writer = self._create_csv_writer(output)
        item = remap(item, visit=self._encode_string)
        writer.writerow(item)
        return output.getvalue().rstrip()

    def format_header(self):
        if self.show_titles:
            output = io.BytesIO()
            writer = self._create_csv_writer(output)
            writer.writeheader()
            return output.getvalue().rstrip() + '\n'

    def format(self, item):
        return self._item_to_csv(item)
