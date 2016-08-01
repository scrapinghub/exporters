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
        self.filter_func = self._create_filter_func(self.filters)
        self.logger.info('MultipleFilter instantiated.')

    def filter(self, item):
        return self.filter_func(item)

    def _create_filter_func(self, filters):
        """ Should generate a list of functions and return a function
        that check if all the functions in list return True """
        filter_functions = []
        for f in filters:
            _filter = f.get('name') or f.get('or')
            if isinstance(_filter, list):
                filter_func = self._create_or_filter(_filter)
            else:
                filter_func = _filter.filter
            filter_functions.append(filter_func)

        def filter_func(item):
            results = [f(item) for f in filter_functions]
            return all(results)

        return filter_func

    def _create_or_filter(self, filters):
        def or_filter(item):
            return any(f['name'].filter(item) for f in filters)
        return or_filter

    def _load_filters(self, filters):
        """ Receives a list of filter options and return a list of filter
        instances """
        filter_instances = []
        for f in filters:
            if f.keys()[0] == 'or':
                filter_instances.append({'or': self._load_filters(f['or'])})
            else:
                filter_instances.append({
                    'name': self.module_loader.load_filter(f, self.metadata)
                })
        return filter_instances
