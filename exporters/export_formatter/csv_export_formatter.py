import csv
import io
from exporters.export_formatter.base_export_formatter import BaseExportFormatter


class CSVExportFormatter(BaseExportFormatter):

    requirements = {
        'show_titles': {'type': bool, 'required': False, 'default': False},
        'delimiter': {'type': basestring, 'required': False, 'default': ','},
        'string_delimiter': {'type': basestring, 'required': False, 'default': '"'},
        'line_end_character': {'type': basestring, 'required': False, 'default': '\n'},
        'columns': {'type': list, 'required': False, 'default': []},
        'titles': {'type': list, 'required': False, 'default': []},
        'null_element': {'type': basestring, 'required': False, 'default': ''}
    }

    def __init__(self, options, settings):
        super(CSVExportFormatter, self).__init__(options, settings)
        self.show_titles = self.read_option('show_titles')
        self.titles_already_shown = False
        self.delimiter = self.read_option('delimiter')
        self.string_delimiter = self.read_option('string_delimiter')
        self.line_end_character = self.read_option('line_end_character')
        self.columns = self.read_option('columns')
        if self.show_titles:
            self.titles = self.read_option('titles')
            if len(self.columns) != len(self.titles):
                raise ValueError('Columns and Titles have different sizes')
        self.null_element = self.read_option('null_element')

    def format(self, batch):
        if self.show_titles and not self.titles_already_shown:
            # Show titles
            item = self.delimiter.join(self.titles) + self.line_end_character
            self.titles_already_shown = True
            yield item

        for item in batch:
            item.formatted = self._item_to_csv(item)
            yield item

    def _item_to_csv(self, item):
        output = io.BytesIO()
        writer = csv.DictWriter(output, fieldnames=item.keys(), delimiter=self.delimiter, quotechar=self.string_delimiter,
                            quoting=csv.QUOTE_NONNUMERIC, lineterminator=self.line_end_character)
        writer.writerow(item)
        return output.getvalue().rstrip()
