DEFAULT_FILTER_CONFIG = {'name': 'ozzy.filters.no_filter.NoFilter', 'options': {}}
DEFAULT_TRANSFORM_CONFIG = {'name': 'ozzy.transform.no_transform.NoTransform', 'options': {}}
DEFAULT_GROUPER_CONFIG = {'name': 'ozzy.groupers.no_grouper.NoGrouper', 'options': {}}
DEFAULT_PERSISTENCE_CONFIG = {
    'name': 'ozzy.persistence.pickle_persistence.PicklePersistence',
    'options': {'file_path': '/tmp/'}
}
DEFAULT_STATS_MANAGER_CCONFIG = {
    'name': 'ozzy.stats_managers.basic_stats_manager.BasicStatsManager',
    'options': {}
}
DEFAULT_FORMATTER_CONFIG = {
    "name": "ozzy.export_formatter.json_export_formatter.JsonExportFormatter",
    "options": {}
}
DEFAULT_LOGGER_LEVEL = 'INFO'
DEFAULT_LOGGER_NAME = 'export-pipeline'
