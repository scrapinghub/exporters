import json

DEFAULT_FILTER_CLASS = {'name': 'exporters.filters.no_filter.NoFilter', 'options': {}}
DEFAULT_TRANSFORM_CLASS = {'name': 'exporters.transform.no_transform.NoTransform', 'options': {}}
DEFAULT_GROUPER_CLASS = {'name': 'exporters.groupers.no_grouper.NoGrouper', 'options': {}}
DEFAULT_PERSISTENCE_CLASS = {'name': 'exporters.persistence.pickle_persistence.PicklePersistence',
                             'options': {'file_path': '/tmp/'}
                             }


class ExporterOptions(object):
    def __init__(self, options):
        self.curate_options(options)
        self.options = options
        self.reader_options = self.options['reader']
        if 'filter' in self.options:
            self.filter_before_options = self.options['filter']
        else:
            self.filter_before_options = self.options.get('filter_before', DEFAULT_FILTER_CLASS)
        self.filter_after_options = self.options.get('filter_after', DEFAULT_FILTER_CLASS)
        self.transform_options = self.options.get('transform', DEFAULT_TRANSFORM_CLASS)
        self.grouper_options = self.options.get('grouper', DEFAULT_GROUPER_CLASS)
        self.writer_options = self.options['writer']
        self.exporter_options = self.options['exporteroptions']
        self.persistence_options = self.options.get('persistence', DEFAULT_PERSISTENCE_CLASS)
        self.formatter_options = self.options['exporteroptions']['formatter']

    def curate_options(self, options):
        if 'reader' not in options:
            raise ValueError('Options must contain a reader definition')
        if 'writer' not in options:
            raise ValueError('Options must contain a writer definition')
        if 'exporteroptions' not in options:
            raise ValueError('Options must contain a exporteroptions definition')

    def __str__(self):
        return json.dumps(self.options)
