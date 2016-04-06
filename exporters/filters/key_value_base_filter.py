from exporters.filters.base_filter import BaseFilter
from exporters.utils import nested_dict_value


class KeyValueBaseFilter(BaseFilter):
    """
    Filter items depending on keys and values

        - keys (list)
            It is a list of dicts with the following structure: {"key": "value"}.
            The filter will delete those items that do not contain a
            key "key" or, if they do, that key is not the same as "value".
    """
    # List of options
    supported_options = {
        'keys': {'type': list},
        'nested_field_separator': {'type': basestring, 'default': '.'}
    }

    def __init__(self, *args, **kwargs):
        super(KeyValueBaseFilter, self).__init__(*args, **kwargs)
        self.keys = self.read_option('keys')
        self.nested_field_separator = self.read_option('nested_field_separator')

    def filter(self, item):
        for key in self.keys:
            if self.nested_field_separator:
                nested_fields = key['name'].split(self.nested_field_separator)
                value = nested_dict_value(item, nested_fields)
            else:
                value = item[key['name']]
            if not self.meets_condition(value, key['value']):
                return
        return item

    def meets_condition(self, value, key_value):
        raise NotImplementedError
