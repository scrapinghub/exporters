from exporters.filters.base_filter import BaseFilter


class KeyValueFilter(BaseFilter):
    """
    Filter items depending on keys and values

        - keys (list)
            It is a list of dicts with the following structure: {"key": "value"}.
            The filter will delete those items that do not contain a
            key "key" or, if they do, that key is not the same as "value".
    """
    # List of options
    supported_options = {
        'keys': {'type': list}
    }

    def __init__(self, *args, **kwargs):
        super(KeyValueFilter, self).__init__(*args, **kwargs)
        self.keys = self.read_option('keys')
        self.logger.info('KeyValueFilter has been initiated. Keys: {}'.format(self.keys))

    def filter(self, item):
        return all(kv['name'] in item and item[kv['name']] == kv['value']
                   for kv in self.keys)
