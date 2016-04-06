from exporters.filters.base_filter import BaseFilter
from exporters.utils import nested_dict_value


class KeyValueBaseFilter(BaseFilter):
    "Base class to key-value filters"

    supported_options = {
        'keys': {'type': list},
        'nested_field_separator': {'type': basestring, 'default': '.'}
    }

    def __init__(self, *args, **kwargs):
        super(KeyValueBaseFilter, self).__init__(*args, **kwargs)
        self.keys = self.read_option('keys')
        self.nested_field_separator = self.read_option('nested_field_separator')
        self.logger.info('{} has been initiated. Keys: {}'.format(
            self.__class__.__name__, self.keys))

    def filter(self, item):
        for key in self.keys:
            if self.nested_field_separator:
                nested_fields = key['name'].split(self.nested_field_separator)
                value = nested_dict_value(item, nested_fields)
            else:
                value = item[key['name']]
            if not self._match_value(value, key['value']):
                return
        return item

    def _match_value(self, value_found, value_expected):
        """Return True if value found matches the expected.
        Should be overriden by derived classes implementing custom match.
        """
        raise NotImplementedError
