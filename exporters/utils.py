import os
import shutil
import tempfile
from contextlib import contextmanager
from exporters.module_loader import ModuleLoader
from exporters.readers.s3_reader import S3BucketKeysFetcher


def remove_if_exists(file_name):
    try: os.remove(file_name)
    except: pass


@contextmanager
def TemporaryDirectory():
    name = tempfile.mkdtemp()
    try:
        yield name
    finally:
        shutil.rmtree(name)


class S3BypassState(object):

    def __init__(self, config):
        self.config = config
        module_loader = ModuleLoader()
        self.state = module_loader.load_persistence(config.persistence_options)
        self.state_position = self.state.get_last_position()
        if not self.state_position:
            self.pending = S3BucketKeysFetcher(self.config.reader_options['options']).pending_keys()
            self.done = []
            self.skipped = []
            self.state.commit_position(self._get_state())
        else:
            self.pending = self.state_position['pending']
            self.done = []
            self.skipped = self.state_position['done']
            self.keys = self.pending

    def _get_state(self):
        return {'pending': self.pending, 'done': self.done, 'skipped': self.skipped}

    def commit_copied_key(self, key):
        self.pending.remove(key)
        self.done.append(key)
        self.state.commit_position(self._get_state())

    def pending_keys(self):
        return self.pending

    def delete(self):
        self.state.delete()