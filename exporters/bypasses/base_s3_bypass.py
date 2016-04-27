import logging
from copy import deepcopy

from exporters.bypasses.base_bypass import RequisitesNotMet, BaseBypass
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
        self.logger = logging.getLogger('bypass_logger')
        self.logger.setLevel(logging.INFO)

    @classmethod
    def meets_conditions(cls, config):
        if not config.reader_options['name'].endswith('S3Reader'):
            raise RequisitesNotMet
        if not config.filter_before_options['name'].endswith('NoFilter'):
            raise RequisitesNotMet('custom filter configured')
        if not config.filter_after_options['name'].endswith('NoFilter'):
            raise RequisitesNotMet('custom filter configured')
        if not config.transform_options['name'].endswith('NoTransform'):
            raise RequisitesNotMet('custom transform configured')
        if not config.grouper_options['name'].endswith('NoGrouper'):
            raise RequisitesNotMet('custom grouper configured')
        if config.writer_options['options'].get('items_limit'):
            raise RequisitesNotMet('items limit configuration (items_limit)')
        if config.writer_options['options'].get('items_per_buffer_write'):
            raise RequisitesNotMet('buffer limit configuration (items_per_buffer_write)')
        if config.writer_options['options'].get('size_per_buffer_write'):
            raise RequisitesNotMet('buffer limit configuration (size_per_buffer_write)')

    def execute(self):
        reader_options = self.config.reader_options['options']
        self.bypass_state = S3BypassState(
            self.config, self.metadata,
            self.read_option('reader', 'aws_access_key_id', 'EXPORTERS_S3READER_AWS_KEY'),
            self.read_option('reader', 'aws_secret_access_key', 'EXPORTERS_S3READER_AWS_SECRET'))
        self.total_items = self.bypass_state.stats['total_count']
        source_bucket = get_bucket(**reader_options)
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

    def _copy_s3_key(key):
        raise NotImplementedError

    def close(self):
        if self.bypass_state:
            self.bypass_state.delete()
