import datetime
import logging
import os
import shutil
from exporters.bypasses.s3_bypass_state import S3BypassState
from exporters.default_retries import retry_long
from exporters.export_formatter.json_export_formatter import JsonExportFormatter
from exporters.export_managers.base_bypass import RequisitesNotMet, BaseBypass
from exporters.readers.s3_reader import get_bucket
from exporters.utils import TmpFile
from exporters.writers.azure_blob_writer import AzureBlobWriter


class AzureBlobS3Bypass(BaseBypass):
    """
    Bypass executed by default when data source is an S3 bucket and data destination is an Azure blob container.
    It should be transparent to user. Conditions are:

        - S3Reader and AzureBlobWriter are used on configuration.
        - No filter modules are set up.
        - No transform module is set up.
        - No grouper module is set up.
        - AzureBlobWriter has not a items_limit set in configuration.
        - AzureBlobWriter has default items_per_buffer_write and size_per_buffer_write per default.
    """

    def __init__(self, config, metadata):
        super(AzureBlobS3Bypass, self).__init__(config, metadata)
        self.tmp_folder = None
        self.bypass_state = None
        self.logger = logging.getLogger('bypass_logger')
        self.logger.setLevel(logging.INFO)

    def _raise_conditions_not_met(self, reason):
        self.logger.warning('Skipping Azure file copy optimization bypass because of %s' % reason)
        raise RequisitesNotMet

    def meets_conditions(self):
        if not self.config.reader_options['name'].endswith('S3Reader') or not self.config.writer_options['name'].endswith('AzureBlobWriter'):
            raise RequisitesNotMet
        if not self.config.filter_before_options['name'].endswith('NoFilter'):
            self._raise_conditions_not_met('custom filter configured')
        if not self.config.filter_after_options['name'].endswith('NoFilter'):
            self._raise_conditions_not_met('custom filter configured')
        if not self.config.transform_options['name'].endswith('NoTransform'):
            self._raise_conditions_not_met('custom transform configured')
        if not self.config.grouper_options['name'].endswith('NoGrouper'):
            self._raise_conditions_not_met('custom grouper configured')
        if self.config.writer_options['options'].get('items_limit'):
            self._raise_conditions_not_met('items limit configuration (items_limit)')
        if self.config.writer_options['options'].get('items_per_buffer_write'):
            self._raise_conditions_not_met('buffer limit configuration (items_per_buffer_write)')
        if self.config.writer_options['options'].get('size_per_buffer_write'):
            self._raise_conditions_not_met('buffer limit configuration (size_per_buffer_write)')

    def _get_filebase(self, writer_options):
        dest_filebase = writer_options['filebase'].format(datetime.datetime.now())
        dest_filebase = datetime.datetime.now().strftime(dest_filebase)
        return dest_filebase

    def _fill_config_with_env(self):
        if 'aws_access_key_id' not in self.config.reader_options['options']:
            self.config.reader_options['options']['aws_access_key_id'] = os.environ.get('EXPORTERS_S3READER_AWS_KEY')
        if 'aws_secret_access_key' not in self.config.reader_options['options']:
            self.config.reader_options['options']['aws_secret_access_key'] = os.environ.get('EXPORTERS_S3READER_AWS_SECRET')

    def bypass(self):
        from copy import deepcopy
        reader_options = self.config.reader_options['options']
        self.writer = AzureBlobWriter(self.config.writer_options, self.metadata, export_formatter=JsonExportFormatter({}))
        self._fill_config_with_env()
        self.bypass_state = S3BypassState(self.config, self.metadata)
        self.total_items = self.bypass_state.stats['total_count']
        source_bucket = get_bucket(**reader_options)
        pending_keys = deepcopy(self.bypass_state.pending_keys())
        try:
            for key in pending_keys:
                self._copy_key(source_bucket, key)
                self.bypass_state.commit_copied_key(key)
                logging.log(logging.INFO,
                            'Copied key {}'.format(key.name))

        finally:
            if self.tmp_folder:
                shutil.rmtree(self.tmp_folder)

    # @retry_long
    def _copy_key(self, source_bucket, key_name):
        akey = source_bucket.get_key(key_name)
        if akey.get_metadata('total'):
            self.increment_items(int(akey.get_metadata('total')))
            self.bypass_state.increment_items(int(akey.get_metadata('total')))
        else:
            self.valid_total_count = False
        key = source_bucket.get_key(key_name)
        with TmpFile() as tmp_filename:
            key.get_contents_to_filename(tmp_filename)
            self.writer.write(tmp_filename)

    def close(self):
        self.bypass_state.delete()
        self.writer.flush()
        self.writer.finish_writing()
        self.writer.close()
