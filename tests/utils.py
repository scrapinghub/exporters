import contextlib
import gzip
import json
import mock
import os
import StringIO
from contextlib import closing
from copy import deepcopy
from ozzy.meta import ExportMeta
from ozzy.persistence.base_persistence import BasePersistence
from ozzy.readers.base_reader import BaseReader
from ozzy.writers.base_writer import BaseWriter


VALID_EXPORTER_CONFIG = {
    'reader': {
        'name': 'ozzy.readers.random_reader.RandomReader',
    },
    'writer': {
        'name': 'ozzy.writers.console_writer.ConsoleWriter',
    },
    'filter': {
        'name': 'ozzy.filters.no_filter.NoFilter',
    },
    'filter_before': {
        'name': 'ozzy.filters.no_filter.NoFilter',
    },
    'filter_after': {
        'name': 'ozzy.filters.no_filter.NoFilter',
    },
    'transform': {
        'name': 'ozzy.transform.no_transform.NoTransform',
    },
    'exporter_options': {},
    'persistence': {
        'name': 'ozzy.persistence.pickle_persistence.PicklePersistence',
        'options': {'file_path': '/tmp'}
    },
    'grouper': {
        'name': 'ozzy.groupers.no_grouper.NoGrouper',
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


class CopyingMagicMock(mock.MagicMock):
    def _mock_call(_mock_self, *args, **kwargs):
        return super(CopyingMagicMock, _mock_self)._mock_call(
            *deepcopy(args), **deepcopy(kwargs))


def create_s3_keys(bucket, key_names):
    for key_name in key_names:
        with closing(bucket.new_key(key_name)) as key:
            out = StringIO.StringIO()
            with gzip.GzipFile(fileobj=out, mode='w') as f:
                f.write(json.dumps({'name': key_name}))
            key.set_contents_from_string(out.getvalue())
