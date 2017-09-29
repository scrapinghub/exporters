from __future__ import absolute_import
import contextlib
import gzip
import json
import mock
import os
from contextlib import closing
from copy import deepcopy
from exporters.meta import ExportMeta
from exporters.persistence.base_persistence import BasePersistence
from exporters.readers.base_reader import BaseReader
from exporters.writers.base_writer import BaseWriter

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO


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
