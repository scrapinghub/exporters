from exporters.filters.base_filter import BaseFilter


class KeyValueFilter(BaseFilter):
    """
    Filter items depending on keys and values

    Parameters
    ----------
    keys : list
        It is a list of dicts with the following structure: {"key": "value"}.
        "value can be either a value itself or an iterable (i.e., list, tuple))
        The filter will delete those items that:
        do not contain a key "key" or
        their value either is not same as "value" (if value is not iterable) or
        their value is not in "value" (if value is an iterable).

    Examples
    --------
    >>> keys = [{'name': 'age', 'value': 25}, {'name': 'country', 'value': ['ES', 'PT']}]
    >>> items = [{'age': 25, 'country': 'ES'}, {'age': 18, 'country': 'US'}, {'country': 'PT'}]
    >>> list(filter(KeyValueFilter.filter, items))
    {'age': 25, 'country': 'ES'}]
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
        for kv in self.keys:
            if kv['name'] not in item:
                return False
            elif hasattr(kv["value"], '__iter__') and not(item[kv['name']] in kv['value']):
                return False
            elif not(hasattr(kv["value"], '__iter__')) and item[kv['name']] != kv['value']:
                return False
        return True
