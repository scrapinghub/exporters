from exporters.filters.base_filter import BaseFilter
from exporters.module_loader import ModuleLoader


class MultipleFilter(BaseFilter):

    supported_options = {
        'filters': {'type': dict, 'default': {}},
        'composition': {'type': basestring, 'default': None}
    }

    def __init__(self, *args, **kwargs):
        super(MultipleFilter, self).__init__(*args, **kwargs)
        self.module_loader = ModuleLoader()
        self.filters = self.load_filters()
        self.composition = self.read_option('composition')

    def load_filters(self):
        filters = {}
        for filter_name, filter_config in self.read_option('filters').iteritems():
            filters[filter_name] = self.module_loader.load_filter(filter_config, self.metadata)
        return filters

    def and_filter(self, item):
        for filter_name, filter_module in self.filters.iteritems():
            if filter_module.filter(item) is False:
                return False
        return True

    def filter(self, item):
        if self.composition is None:
            return self.and_filter(item)
        return True
