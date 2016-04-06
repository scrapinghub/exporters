import re
from exporters.filters.key_value_base_filter import KeyValueBaseFilter


class KeyValueRegexFilter(KeyValueBaseFilter):
    """
    Filter items depending on keys and values using regular expressions

        - keys (list)
            It is a list of dicts with the following structure: {"key": "regex"}.
            The filter will delete those items that do not contain a
            key "key" or, if they do, that key value does not match "regex".
    """
    def _match_value(self, found, expected):
        return bool(re.match(expected, found))
