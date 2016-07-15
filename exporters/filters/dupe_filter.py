from exporters.filters.base_filter import BaseFilter


class DupeFilter(BaseFilter):
    """
    Filter items depending on a key field.

        - key_field (str)
            item's key to be used to identify dupes
    """
    # List of options
    supported_options = {
        'key_field': {'type': basestring, 'default': '_key'}
    }

    def __init__(self, *args, **kwargs):
        super(DupeFilter, self).__init__(*args, **kwargs)
        self.key_field = self.read_option('key_field')
        self.key_set = set()
        self.logger.info('{} initialized. Key field: "{}"'.format(
            self.__class__.__name__, self.key_field))

    def filter(self, item):
        items_key = item.get(self.key_field)
        if not items_key:  # unable to determine duplicates, won't be filtered
            self.logger.warning('Item without "key" found,'
                                ' unable to filter it.')
            return True

        if items_key not in self.key_set:
            self.key_set.add(items_key)
            return True
        return False
