from itertools import izip_longest
from exporters.filters.base_filter import BaseFilter
from exporters.module_loader import ModuleLoader
from exporters.utils import dict_list


def grouper(n, iterable, fillvalue=None):
    "grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)


class MultipleFilter(BaseFilter):
    """
    Apply multiple filters to an Item, the filters can be combined. The way
    this filter works is very similar to [Mongo Query](
    https://docs.mongodb.com/manual/tutorial/query-documents)

        - filters (list of dict)
            Filters to be applied to each item

    Example:
        "filters": [
            {"name": "exporters.filters.key_value_regex_filter.KeyValueRegexFilter",
             "options": {
                 "keys": [
                     {"name": "country", "value": "United States"}
                 ]
             }
            },
            {"name": "exporters.filters.key_value_regex_filter.KeyValueRegexFilter",
             "options": {
                 "keys": [
                     {"name": "city", "value": "New York"}
                 ]
             }
            },
            {"or": [
                {"name": "exporters.filters.key_value_regex_filter.KeyValueRegexFilter",
                 "options": {
                     "keys": [
                         {"name": "country", "value": "Canada"}
                     ]
                 }
                },
                {"name": "exporters.filters.key_value_regex_filter.KeyValueRegexFilter",
                 "options": {
                     "keys": [
                         {"name": "city", "value": "Montreal"}
                     ]
                 }
                },
            ]}
        ]

        The config above could be translated to:
        (country == 'United States' and city == 'New York') or
        (country == 'Canada' and city == 'Montreal')
    """

    # List of options
    supported_options = {
        'filters': {'type': dict_list},
    }

    def __init__(self, *args, **kwargs):
        super(MultipleFilter, self).__init__(*args, **kwargs)
        self.module_loader = ModuleLoader()
        self.filter_options = self.read_option('filters')
        self.filters = self._load_filters(self.filter_options)
        self.logger.info('MultipleFilter instantiated.')

    def filter(self, item):
        return self._parse_filter(item, self.filters)

    def _parse_filter(self, item, filters, operator='and'):
        """ This function will parse 2 filters at time and evaluate them after
        all filters were evaluated it returns the final evaluation """
        last_evaluation = None
        for left, right in grouper(2, filters, fillvalue={}):
            left_filter = left.get('name') or left.get('or')
            right_filter = right.get('name') or right.get('or')

            if not isinstance(left_filter, list):
                left_result = left_filter.filter(item)
            else:
                left_result = self._parse_filter(item, left_filter, 'or')

            if right and not isinstance(right_filter, list):
                right_result = right_filter.filter(item)
            elif right and right_filter:
                right_result = self._parse_filter(item, right_filter, 'or')
            else:
                right_result = last_evaluation or left_result

            if operator == 'and':
                evaluation = left_result and right_result
            elif operator == 'or':
                evaluation = left_result or right_result

            last_evaluation = evaluation

        return last_evaluation

    def _load_filters(self, filters):
        """ Receives a list of filter options and return a dict where the key
        is the filter name and the value is it's instance """
        filter_instances = []
        for f in filters:
            if f.keys()[0] == 'or':
                filter_instances.append({'or': self._load_filters(f['or'])})
            else:
                filter_instances.append({
                    'name': self.module_loader.load_filter(f, self.metadata)
                })
        return filter_instances
