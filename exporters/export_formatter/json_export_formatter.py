import json
from exporters.export_formatter.base_export_formatter import BaseExportFormatter


class JsonExportFormatter(BaseExportFormatter):
    """
    This export formatter provides a way of exporting items in JSON format. This one is the
    default formatter.

        - pretty_print(bool)
            If set to True, items will be exported with an ident of 2 and keys sorted, they
            will exported with a text line otherwise.
    """

    supported_options = {
        'pretty_print': {'type': bool, 'default': False}
    }

    file_extension = 'jl'

    def __init__(self, options):
        super(JsonExportFormatter, self).__init__(options)
        self.pretty_print = self.read_option('pretty_print')

    def format(self, item):
        options = dict(indent=2, sort_keys=True) if self.pretty_print else dict()
        return json.dumps(item, **options)
