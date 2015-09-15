import json

DEFAULT_FILTER_CLASS = {'name': 'exporters.filters.no_filter.NoFilter', 'options': {}}
DEFAULT_TRANSFORM_CLASS = {'name': 'exporters.transform.no_transform.NoTransform', 'options': {}}
DEFAULT_GROUPER_CLASS = {'name': 'exporters.groupers.no_grouper.NoGrouper', 'options': {}}
DEFAULT_PERSISTENCE_CLASS = {'name': 'exporters.persistence.pickle_persistence.PicklePersistence',
                             'options': {'file_path': '/tmp/'}
                             }
DEFAULT_STATS_MANAGER_CLASS = {
    'name': 'exporters.stats_managers.basic_stats_manager.BasicStatsManager',
    'options': {}
}
DEFAULT_FORMATTER_CLASS = {"name": "exporters.export_formatter.json_export_formatter.JsonExportFormatter",
                           "options": {}}


class ExporterConfig(object):
    def __init__(self, configuration):
        self.curate_configuration(configuration)
        self.configuration = configuration
        self.reader_options = self.configuration['reader']
        if 'filter' in self.configuration:
            self.filter_before_options = self.configuration['filter']
        else:
            self.filter_before_options = self.configuration.get('filter_before', DEFAULT_FILTER_CLASS)
        self.filter_after_options = self.configuration.get('filter_after', DEFAULT_FILTER_CLASS)
        self.transform_options = self.configuration.get('transform', DEFAULT_TRANSFORM_CLASS)
        self.grouper_options = self.configuration.get('grouper', DEFAULT_GROUPER_CLASS)
        self.writer_options = self.configuration['writer']
        self.exporter_options = self.configuration['exporter_options']
        self.persistence_options = self.configuration.get('persistence', DEFAULT_PERSISTENCE_CLASS)
        self.formatter_options = self.configuration['exporter_options'].get('formatter', DEFAULT_FORMATTER_CLASS)
        self.stats_options = self.configuration['exporter_options'].get('stats_manager', DEFAULT_STATS_MANAGER_CLASS)

    def curate_configuration(self, configuration):
        if 'reader' not in configuration:
            raise ValueError('Configuration must contain a reader definition')
        if 'writer' not in configuration:
            raise ValueError('Configuration must contain a writer definition')
        if 'exporter_options' not in configuration:
            raise ValueError('Configuration must contain a exporter_options definition')

    def __str__(self):
        return json.dumps(self.configuration)
