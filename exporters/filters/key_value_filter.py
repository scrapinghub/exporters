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

    def __init__(self, options):
        super(KeyValueFilter, self).__init__(options)
        self.keys = self.read_option('keys')
        self.logger.info('KeyValueFilter has been initiated. Keys: {}'.format(self.keys))

    def filter(self, item):
        for kv in self.keys:
            if kv['name'] not in item:
                return False
            elif isinstance(kv['value'], list) and not(item[kv['name']] in kv['value']):
                return False
            elif not(isinstance(kv['value'], list)) and item[kv['name']] != kv['value']:
                return False
        return True
