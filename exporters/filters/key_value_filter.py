from exporters.filters.key_value_base_filter import KeyValueBaseFilter


class KeyValueFilter(KeyValueBaseFilter):
    """
    Filter items depending on keys and values

        - keys (list)
            It is a list of dicts with the following structure: {"key": "value"}.
            The filter will delete those items that do not contain a
            key "key" or, if they do, that key is not the same as "value".
    """
    def _match_value(self, found, expected):
        return found == expected
