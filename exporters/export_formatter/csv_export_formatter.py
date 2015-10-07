import csv
import io
import six
from exporters.export_formatter.base_export_formatter import BaseExportFormatter
from exporters.records.base_record import BaseRecord


class CSVExportFormatter(BaseExportFormatter):
    supported_options = {
        'show_titles': {'type': bool, 'default': False},
        'delimiter': {'type': basestring, 'default': ','},
        'string_delimiter': {'type': basestring, 'default': '"'},
        'line_end_character': {'type': basestring, 'default': '\n'},
        'null_element': {'type': basestring, 'default': ''},
        'fields': {'type': list, 'default': []}
    }

    def __init__(self, options):
        super(CSVExportFormatter, self).__init__(options)
        self.show_titles = self.read_option('show_titles')
        self.titles_already_shown = False
        self.delimiter = self.read_option('delimiter')
        self.string_delimiter = self.read_option('string_delimiter')
        self.line_end_character = self.read_option('line_end_character')
        self.columns = self.read_option('columns')
        self.null_element = self.read_option('null_element')
        self.fields = self.read_option('fields')

    def _write_titles(self, item):
        output = io.BytesIO()
        writer = csv.DictWriter(output, fieldnames=self.fields, delimiter=self.delimiter,
                            quotechar=self.string_delimiter,
                            quoting=csv.QUOTE_NONNUMERIC,
                            lineterminator=self.line_end_character,extrasaction='ignore')
        writer.writeheader()
        header = BaseRecord({})
        header.formatted = output.getvalue().rstrip()
        self.titles_already_shown = True
        return header

    def format(self, batch):
        for item in batch:
            if self.show_titles and not self.titles_already_shown:
                yield self._write_titles(item)
            item.formatted = self._item_to_csv(item)
            yield item

    def _encode_string(self, path, key, value):
        if isinstance(value, six.text_type):
            return key, value.encode('utf-8')
        return key, value

    def _item_to_csv(self, item):
        from boltons.iterutils import remap
        output = io.BytesIO()
        writer = csv.DictWriter(output, fieldnames=self.fields, delimiter=self.delimiter,
                                quotechar=self.string_delimiter,
                                quoting=csv.QUOTE_NONNUMERIC,
                                lineterminator=self.line_end_character,extrasaction='ignore')

        item = remap(item, visit=self._encode_string)
        writer.writerow(item)
        return output.getvalue().rstrip()
