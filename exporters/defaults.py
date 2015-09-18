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
DEFAULT_LOGGER_LEVEL = 'INFO'
DEFAULT_LOGGER_NAME = 'export-pipeline'
