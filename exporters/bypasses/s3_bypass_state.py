from exporters.module_loader import ModuleLoader
from exporters.readers.s3_reader import S3BucketKeysFetcher


class S3BypassState(object):

    def __init__(self, config, metadata, aws_key, aws_secret):
        self.config = config
        module_loader = ModuleLoader()
        self.state = module_loader.load_persistence(config.persistence_options, metadata)
        self.state_position = self.state.get_last_position()
        if not self.state_position:
            self.pending = S3BucketKeysFetcher(
                self.config.reader_options['options'], aws_key, aws_secret).pending_keys()
            self.done = []
            self.skipped = []
            self.stats = {'total_count': 0}
            self.state.commit_position(self._get_state())
        else:
            self.pending = self.state_position['pending']
            self.done = []
            self.skipped = self.state_position['done']
            self.keys = self.pending
            self.stats = self.state_position.get('stats', {'total_count': 0})

    def _get_state(self):
        return dict(pending=self.pending, done=self.done, skipped=self.skipped,
                    stats=self.stats)

    def commit_copied_key(self, key):
        self.pending.remove(key)
        self.done.append(key)
        self.state.commit_position(self._get_state())

    def increment_items(self, items_number):
        self.stats['total_count'] += items_number

    def pending_keys(self):
        return self.pending

    def delete(self):
        self.state.delete()
