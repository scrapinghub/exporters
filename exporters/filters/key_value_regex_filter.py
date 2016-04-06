import warnings
from .key_value_filters import KeyValueRegexFilter  # NOQA


warnings.warn('Module exporters.filters.key_value_regex_filter has been deprecated,'
              ' use exporters.filters.key_values_filters instead')
