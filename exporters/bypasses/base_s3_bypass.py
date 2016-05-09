import logging
from copy import deepcopy
from exporters.bypasses.base import BaseBypass
from exporters.bypasses.s3_bypass_state import S3BypassState
from exporters.readers.s3_reader import get_bucket


class BaseS3Bypass(BaseBypass):
    """
    Bypass executed by default when data source is an S3 bucket.
    It should be transparent to user. Conditions are:

        - S3Reader is used on configuration.
        - No filter modules are set up.
        - No transform module is set up.
        - No grouper module is set up.
        - writer has no option items_limit set in configuration.
        - writer has default items_per_buffer_write and size_per_buffer_write per default.
    """

    def __init__(self, config, metadata):
        super(BaseS3Bypass, self).__init__(config, metadata)
        self.bypass_state = None

    @classmethod
    def meets_conditions(cls, config):
        if not config.reader_options['name'].endswith('S3Reader'):
            cls._log_skip_reason('Wrong reader configured')
            return False
        if not config.filter_before_options['name'].endswith('NoFilter'):
            cls._log_skip_reason('custom filter configured')
            return False
        if not config.filter_after_options['name'].endswith('NoFilter'):
            cls._log_skip_reason('custom filter configured')
            return False
        if not config.transform_options['name'].endswith('NoTransform'):
            cls._log_skip_reason('custom transform configured')
            return False
        if not config.grouper_options['name'].endswith('NoGrouper'):
            cls._log_skip_reason('custom grouper configured')
            return False
        if config.writer_options['options'].get('items_limit'):
            cls._log_skip_reason('items limit configuration (items_limit)')
            return False
        if config.writer_options['options'].get('items_per_buffer_write'):
            cls._log_skip_reason('buffer limit configuration (items_per_buffer_write)')
            return False
        if config.writer_options['options'].get('size_per_buffer_write'):
            cls._log_skip_reason('buffer limit configuration (size_per_buffer_write)')
            return False
        return True

    def execute(self):
        reader_aws_key = self.read_option('reader', 'aws_access_key_id')
        reader_aws_secret = self.read_option('reader', 'aws_secret_access_key')
        self.bypass_state = S3BypassState(
            self.config, self.metadata,
            reader_aws_key,
            reader_aws_secret)
        self.total_items = self.bypass_state.stats['total_count']
        source_bucket = get_bucket(
            self.read_option('reader', 'bucket'), reader_aws_key, reader_aws_secret)
        keys_to_copy = deepcopy(self.bypass_state.pending_keys())
        for key in keys_to_copy:
            self._copy_key(source_bucket, key)
            self.bypass_state.commit_copied_key(key)
            logging.log(logging.INFO, 'Copied key {}'.format(key))

    def _copy_key(self, source_bucket, key_name):
        key = source_bucket.get_key(key_name)
        if key.get_metadata('total'):
            total = int(key.get_metadata('total'))
            self.increment_items(total)
            self.bypass_state.increment_items(total)
        else:
            self.valid_total_count = False
        self._copy_s3_key(key)

    def _copy_s3_key(self, key):
        raise NotImplementedError

    def close(self):
        if self.bypass_state:
            self.bypass_state.delete()
