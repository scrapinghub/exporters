import csv
import io
import six
from exporters.exceptions import ConfigurationError
from exporters.export_formatter.base_export_formatter import BaseExportFormatter
from exporters.records.base_record import BaseRecord


class CSVExportFormatter(BaseExportFormatter):

    format_name = 'csv'

    supported_options = {
        'show_titles': {'type': bool, 'default': True},
        'fields': {'type': list, 'default': []},
        'schema': {'type': dict, 'default': {}}
    }

    def __init__(self, options):
        super(CSVExportFormatter, self).__init__(options)
        self.show_titles = self.read_option('show_titles')
        self.titles_already_shown = False
        self.fields = self._get_fields()

    def _get_fields_from_schema(self):
        schema = self.read_option('schema')
        return schema.get('properties', {}).keys()

    def _get_fields(self):
        if self.read_option('fields'):
            return self.read_option('fields')
        elif not self.read_option('schema'):
            raise ConfigurationError('CSV formatter requires at least one of: fields or schema')
        return self._get_fields_from_schema()

    def _write_titles(self):
        output = io.BytesIO()
        writer = self._create_csv_writer(output)
        writer.writeheader()
        header = BaseRecord({})
        header.formatted = output.getvalue().rstrip()
        header.format = self.format_name
        header.header = True
        self.titles_already_shown = True
        return header

    def format(self, batch):
        for item in batch:
            if self.show_titles and not self.titles_already_shown:
                yield self._write_titles()
            item.formatted = self._item_to_csv(item)
            item.format = self.format_name
            yield item

    def _encode_string(self, path, key, value):
        if isinstance(value, six.text_type):
            return key, value.encode('utf-8')
        return key, value

    def _create_csv_writer(self, outputf):
        return csv.DictWriter(outputf, fieldnames=self.fields,
                              quoting=csv.QUOTE_NONNUMERIC,
                              extrasaction='ignore')

    def _item_to_csv(self, item):
        from boltons.iterutils import remap
        output = io.BytesIO()
        writer = self._create_csv_writer(output)
        item = remap(item, visit=self._encode_string)
        writer.writerow(item)
        return output.getvalue().rstrip()
