import json
import six
import io

from exporters.exceptions import ConfigurationError
from exporters.export_formatter.base_export_formatter import BaseExportFormatter


class AvroExportFormatter(BaseExportFormatter):
    """
    This export formatter provides a way of exporting items in AVRO format.
    follows the avro schema 1.8 https://avro.apache.org/docs/1.8.1/spec.html
    We write the items on a 1 per block way.
    This allows for streaming and keeps the files consistency even if the process
    ends unexpectedly

        - schema(string)
            json-encoded schema for the data to use in avro formatter

        - schema_path(string)
            path to the json schema for the data to use in avro formatter.
            Useful for large schemas if readibility of the job json file is important
    """

    supported_options = {
        'schema_path': {'type': six.string_types, 'default': None},
        'schema': {'type': (dict, list), 'default': None},
    }
    file_extension = 'avro'
    item_separator = ''

    def __init__(self, *args, **kwargs):
        super(AvroExportFormatter, self).__init__(*args, **kwargs)
        self.schema = self._get_schema()
        self._setup_avrowriter()

    def _get_schema(self):
        if self.read_option('schema_path'):
            with open(self.read_option('schema_path')) as fname:
                return json.load(fname)
        elif self.read_option('schema'):
            return self.read_option('schema')
        else:
            raise ConfigurationError(
                    'Avro formatter requires at least one of: schema or schema_path')

    def _setup_avrowriter(self):
        from fastavro.writer import Writer
        header_buffer = io.BytesIO()
        self.writer = Writer(fo=header_buffer, schema=self.schema, sync_interval=0)
        self.header_value = header_buffer.getvalue()
        self._clear_buffer()

    def format_header(self):
        return self.header_value

    def _clear_buffer(self):
        self.writer.fo.seek(0)
        self.writer.fo.truncate()

    def format(self, item):
        self.writer.write(item)
        item_value = self.writer.fo.getvalue()
        self._clear_buffer()
        return item_value
