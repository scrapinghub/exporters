import json
from exporters.exceptions import ConfigurationError
from exporters.defaults import DEFAULT_FILTER_CLASS, DEFAULT_GROUPER_CLASS, DEFAULT_PERSISTENCE_CLASS, \
    DEFAULT_STATS_MANAGER_CLASS, DEFAULT_FORMATTER_CLASS, DEFAULT_LOGGER_LEVEL, DEFAULT_LOGGER_NAME, \
    DEFAULT_TRANSFORM_CLASS


class ExporterConfig(object):
    def __init__(self, configuration):
        self.configuration = configuration
        self.curate_configuration(configuration)
        self.exporter_options = self.configuration['exporter_options']
        self.reader_options = self._merge_options('reader')
        if 'filter' in self.configuration:
            self.filter_before_options = self._merge_options('filter')
        else:
            self.filter_before_options = self._merge_options('filter_before', DEFAULT_FILTER_CLASS)
        self.filter_after_options = self._merge_options('filter_after', DEFAULT_FILTER_CLASS)
        self.transform_options = self._merge_options('transform', DEFAULT_TRANSFORM_CLASS)
        self.grouper_options = self._merge_options('grouper', DEFAULT_GROUPER_CLASS)
        self.writer_options = self._merge_options('writer')
        self.persistence_options = self._merge_options('persistence', DEFAULT_PERSISTENCE_CLASS)
        self.stats_options = self._merge_options('stats_manager', DEFAULT_STATS_MANAGER_CLASS)
        self.formatter_options = self.configuration['exporter_options'].get('formatter', DEFAULT_FORMATTER_CLASS)
        self.notifiers = self.configuration['exporter_options'].get('notifications', [])

    def curate_configuration(self, configuration):
        if 'reader' not in configuration:
            raise ConfigurationError('Configuration must contain a reader definition')
        if 'writer' not in configuration:
            raise ConfigurationError('Configuration must contain a writer definition')
        if 'exporter_options' not in configuration:
            raise ConfigurationError('Configuration must contain a exporter_options definition')

    def __str__(self):
        return json.dumps(self.configuration)

    def _merge_options(self, module_name, default=None):
        options = self.configuration.get(module_name, default)
        options.update(self.log_options)
        return options

    @property
    def log_options(self):
        return {'log_level': self.exporter_options.get('log_level', DEFAULT_LOGGER_LEVEL),
                'logger_name': self.exporter_options.get('logger_name', DEFAULT_LOGGER_NAME)}
