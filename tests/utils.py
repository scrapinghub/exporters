import contextlib
import os
from copy import deepcopy
from exporters.meta import ExportMeta


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
        'options': {'file_path': '/tmp'}
    },
    'grouper': {
        'name': 'exporters.groupers.no_grouper.NoGrouper',
    }
}


def valid_config_with_updates(updates):
    config = deepcopy(VALID_EXPORTER_CONFIG)
    config.update(updates)
    return config


@contextlib.contextmanager
def environment(env):
    old_env = os.environ
    try:
        os.environ = env
        yield
    finally:
        os.environ = old_env


def meta():
    return ExportMeta(None)
