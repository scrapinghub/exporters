VALID_EXPORTER_CONFIG = {
    'reader': {
        'name': 'exporters.readers.random_reader.RandomReader',
    },
    'writer': {
        'name': 'exporters.writers.console_writer.ConsoleWriter',
    },
    'filter': {
        'name': 'exporters.filters.no_filter.NoFilter',
    },
    'filter_before': {
        'name': 'exporters.filters.no_filter.NoFilter',
    },
    'filter_after': {
        'name': 'exporters.filters.no_filter.NoFilter',
    },
    'transform': {
        'name': 'exporters.transform.no_transform.NoTransform',
    },
    'exporter_options': {},
    'persistence': {
        'name': 'exporters.persistence.pickle_persistence.PicklePersistence',
        'options': {'file_base': '/tmp'}
    },
    'grouper': {
        'name': 'exporters.grouper.no_grouper.NoGrouper',
    }
}


def valid_config_with_updates(updates):
    config = VALID_EXPORTER_CONFIG.copy()
    config.update(updates)
    return config
