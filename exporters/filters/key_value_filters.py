import re

from exporters.filters.base_filter import BaseFilter
from exporters.utils import nested_dict_value
from exporters.utils import dict_list
import operator


class InvalidOperator(ValueError):
    """
    Exception to be trown when an invalid operator is set in filter keys
    """


OPERATORS = {
    'in': lambda a, b: operator.contains(b, a),
    'contains': lambda a, b: b in a,
    '==': lambda a, b: a == b,
    're_match': lambda a, b: bool(re.match(b, u'%s' % a))
}

DEFAULT_OPERATOR = '=='


class KeyValueBaseFilter(BaseFilter):
    "Base class to key-value filters"

    supported_options = {
        'keys': {'type': dict_list},
        'nested_field_separator': {'type': basestring, 'default': '.'}
    }

    def __init__(self, *args, **kwargs):
        super(KeyValueBaseFilter, self).__init__(*args, **kwargs)
        self.keys = self.read_option('keys')
        self.nested_field_separator = self.read_option('nested_field_separator')
        self._validate_keys_operator()
        self.logger.info('{} has been initiated. Keys: {}'.format(
            self.__class__.__name__, self.keys))

    def _validate_keys_operator(self):
        for key in self.keys:
            op = key.get('operator')
            if op and op not in OPERATORS:
                raise InvalidOperator('{} operator not valid in key {}'.format(op, key))

    def filter(self, item):
        for key in self.keys:
            if self.nested_field_separator:
                nested_fields = key['name'].split(self.nested_field_separator)
                try:
                    value = nested_dict_value(item, nested_fields)
                except KeyError:
                    self.logger.debug('Missing path {} from item. Item dismissed'.format(
                            nested_fields))
                    return False
            else:
                value = item[key['name']]
            if not self._match_value(
                    value, key['value'], OPERATORS[key.get('operator', DEFAULT_OPERATOR)]):
                return False
        return True

    def _match_value(self, value_found, value_expected, op=None):
        """Return True if value found matches the expected.
        Should be overriden by derived classes implementing custom match.
        """
        raise NotImplementedError


class KeyValueFilter(KeyValueBaseFilter):
    """
    Filter items depending on keys and values

        - keys (list)
            It is a list of dicts with the following structure: {"key": "value"}.
            The filter will delete those items that do not contain a
            key "key" or, if they do, that key is not the same as "value".
    """
    def _match_value(self, found, expected, op):
        return op(found, expected)


class KeyValueRegexFilter(KeyValueBaseFilter):
    """
    Filter items depending on keys and values using regular expressions

        - keys (list)
            It is a list of dicts with the following structure: {"key": "regex"}.
            The filter will delete those items that do not contain a
            key "key" or, if they do, that key value does not match "regex".
    """
    def _match_value(self, found, expected, op):
        if found is None:
            return False
        return OPERATORS['re_match'](found, expected)
