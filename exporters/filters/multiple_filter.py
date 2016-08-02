from exporters.filters.base_filter import BaseFilter
from exporters.module_loader import ModuleLoader
from exporters.utils import dict_list


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
                     {"name": "status", "value": "Verified"}
                 ]
             }
            },
            {"or": [
                {"name": "exporters.filters.key_value_regex_filter.KeyValueRegexFilter",
                 "options": {
                     "keys": [
                         {"name": "city", "value": "New York"}
                     ]
                 }
                },
                {"name": "exporters.filters.key_value_regex_filter.KeyValueRegexFilter",
                 "options": {
                     "keys": [
                         {"name": "city", "value": "Los Angeles"}
                     ]
                 }
                },
            ]}
        ]

        The config above could be translated to:
        country == 'United States' and status == 'Verified' and
        (city == 'New York' or city == 'Los Angeles')
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
        operator = 'or' if self.filters[0].keys()[0] == 'or' else 'and'
        self.filter_func = self._create_filter_func(self.filters, operator)
        self.logger.info('MultipleFilter instantiated.')

    def filter(self, item):
        return self.filter_func(item)

    def _create_filter_func(self, filters, operator='and'):
        functions = []
        for f in filters:
            nested_filters = f.get('and') or f.get('or')
            if nested_filters:
                _operator = f.keys()[0]
                functions.append(self._create_filter_func(
                    nested_filters, _operator))
            elif f.get('name'):
                functions.append(f['name'].filter)

        def or_filter(item):
            return any(f(item) for f in functions)

        def and_filter(item):
            return all(f(item) for f in functions)

        return and_filter if operator == 'and' else or_filter

    def _load_filters(self, filters):
        """ Receives a list of filter options and return a list of filter
        instances """
        filter_instances = []
        try:
            for f in filters:
                if f.keys()[0] in ('or', 'and'):
                    key = f.keys()[0]
                    filter_instances.append({key: self._load_filters(f[key])})
                else:
                    filter_instances.append({
                        'name': self.module_loader.load_filter(f, self.metadata)
                    })
        except (IndexError, KeyError):
            # IndexError occurs in f.keys()[0] if f is a empty dict
            # KeyError occurs if we call to load_filter with an invalid dict
            # like {'invalid': 'x'}
            raise ValueError('Invalid filter option {!r}'.format(f))
        return filter_instances
