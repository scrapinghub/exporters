import json

import datetime
from collections import Counter

from exporters.export_formatter.base_export_formatter import BaseExportFormatter


def default(o):
    if isinstance(o, datetime.datetime):
        return o.isoformat()
    return json.JSONEncoder.default(o)


class JsonExportFormatter(BaseExportFormatter):
    """
    This export formatter provides a way of exporting items in JSON format. This one is the
    default formatter.

        - pretty_print(bool)
            If set to True, items will be exported with an ident of 2 and keys sorted, they
            will exported with a text line otherwise.
    """

    supported_options = {
        'pretty_print': {'type': bool, 'default': False},
        'jsonlines': {'type': bool, 'default': True}
    }

    file_extension = 'jl'

    def __init__(self, *args, **kwargs):
        super(JsonExportFormatter, self).__init__(*args, **kwargs)
        self.pretty_print = self.read_option('pretty_print')
        self.jsonlines = self.read_option('jsonlines')
        if not self.jsonlines:
            self.file_extension = 'json'
        self.set_metadata('formatted_items', Counter())

    def format(self, item):
        options = dict(indent=2, sort_keys=True) if self.pretty_print else dict()
        line = json.dumps(item, default=default, **options)
        formatted_items = self.get_metadata('formatted_items')
        if not self.jsonlines and formatted_items.get(item.group_membership):
            line = ',' + line
        formatted_items[item.group_membership] += 1
        return line

    def format_header(self):
        if self.jsonlines:
            return ''
        return '['

    def format_footer(self):
        if self.jsonlines:
            return ''
        return ']'
