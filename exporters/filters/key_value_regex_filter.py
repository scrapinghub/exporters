from exporters.filters.base_filter import BaseFilter
import re


class KeyValueRegexFilter(BaseFilter):
    """
    Filter items depending on keys and values using regular expressions

        - keys (list)
            It is a list of dicts with the following structure: {"key": "regex"}.
            The filter will delete those items that do not contain a
            key "key" or, if they do, that key value does not match "regex".
    """
    supported_options = {
        'keys': {'type': list}
    }

    def __init__(self, *args, **kwargs):
        # List of options
        super(KeyValueRegexFilter, self).__init__(*args, **kwargs)
        self.keys = self.read_option('keys')
        self.logger.info('KeyValueRegexFilter has been initiated. Keys: {}'.format(self.keys))

    def filter(self, item):
        return all(kv['name'] in item and re.match(kv['value'], u'%s' % item[kv['name']])
                   for kv in self.keys)
