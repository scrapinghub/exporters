from copy import deepcopy
from exporters.meta import ExportMeta
from exporters.persistence.base_persistence import BasePersistence
from exporters.readers.base_reader import BaseReader
from exporters.writers.base_writer import BaseWriter


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


def meta():
    return ExportMeta(None)


class NullWriter(BaseWriter):
    def write(self, *args, **kwargs):
        """
        Everything goes into /dev/null though items should be counted
        """


class ErrorWriter(BaseWriter):
    msg = "ErrorWriter error"

    def write(self, *args, **kwargs):
        raise RuntimeError(self.msg)


class ErrorReader(BaseReader):
    msg = "ErrorReader error"

    def get_next_batch(self, *args, **kwargs):
        raise RuntimeError(self.msg)


class NullPersistence(BasePersistence):
    def generate_new_job(self):
        return None

    def commit_position(self, *args, **kwargs):
        """
        Just ignoring position
        """

    def get_last_position(self):
        return None

    def close(self):
        pass
