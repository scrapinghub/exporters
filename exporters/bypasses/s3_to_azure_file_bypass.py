import datetime
import logging
import os
import shutil

from exporters.bypasses.s3_bypass_state import S3BypassState
from exporters.default_retries import retry_long
from exporters.export_managers.base_bypass import BaseBypass
from exporters.readers.s3_reader import get_bucket
from exporters.utils import TmpFile


class AzureFileS3Bypass(BaseBypass):
    """
    Bypass executed by default when data source is an S3 bucket and data destination
    is an Azure share.
    It should be transparent to user. Conditions are:

        - S3Reader and AzureFileWriter are used on configuration.
        - No filter modules are set up.
        - No transform module is set up.
        - No grouper module is set up.
        - AzureFileWriter has not a items_limit set in configuration.
        - AzureFileWriter has default items_per_buffer_write and size_per_buffer_write per default.
    """

    def __init__(self, config, metadata):
        super(AzureFileS3Bypass, self).__init__(config, metadata)
        self.tmp_folder = None
        self.bypass_state = None
        self.logger = logging.getLogger('bypass_logger')
        self.logger.setLevel(logging.INFO)

    def _handle_conditions_not_met(self, reason):
        self.logger.warning('Skipping Azure file copy optimization bypass because of %s' % reason)
        return False

    def meets_conditions(self):
        if (not self.config.reader_options['name'].endswith('S3Reader') or
                not self.config.writer_options['name'].endswith('AzureFileWriter')):
            return False
        if not self.config.filter_before_options['name'].endswith('NoFilter'):
            return self._handle_conditions_not_met('custom filter configured')
        if not self.config.filter_after_options['name'].endswith('NoFilter'):
            return self._handle_conditions_not_met('custom filter configured')
        if not self.config.transform_options['name'].endswith('NoTransform'):
            return self._handle_conditions_not_met('custom transform configured')
        if not self.config.grouper_options['name'].endswith('NoGrouper'):
            return self._handle_conditions_not_met('custom grouper configured')
        if self.config.writer_options['options'].get('items_limit'):
            return self._handle_conditions_not_met('items limit configuration (items_limit)')
        if self.config.writer_options['options'].get('items_per_buffer_write'):
            return self._handle_conditions_not_met(
                    'buffer limit configuration (items_per_buffer_write)')
        if self.config.writer_options['options'].get('size_per_buffer_write'):
            return self._handle_conditions_not_met(
                    'buffer limit configuration (size_per_buffer_write)')
        return True

    def _get_filebase(self, writer_options):
        dest_filebase = writer_options['filebase'].format(datetime.datetime.now())
        dest_filebase = datetime.datetime.now().strftime(dest_filebase)
        return dest_filebase

    def _fill_config_with_env(self):
        reader_opts = self.config.reader_options['options']
        if 'aws_access_key_id' not in reader_opts:
            reader_opts['aws_access_key_id'] = os.environ.get('EXPORTERS_S3READER_AWS_KEY')
        if 'aws_secret_access_key' not in self.config.reader_options['options']:
            reader_opts['aws_secret_access_key'] = os.environ.get('EXPORTERS_S3READER_AWS_SECRET')

    def bypass(self):
        from azure.storage.file import FileService
        from copy import deepcopy
        reader_options = self.config.reader_options['options']
        writer_options = self.config.writer_options['options']
        self.share = writer_options['share']
        self.filebase = self.create_filebase_name(writer_options['filebase'])
        self.azure_service = FileService(
            writer_options['account_name'], writer_options['account_key'])
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
                            'Copied key {}'.format(key))
        finally:
            if self.tmp_folder:
                shutil.rmtree(self.tmp_folder)

    def create_filebase_name(self, filebase):
        formatted_filebase = datetime.datetime.now().strftime(filebase)
        filebase_path, prefix = os.path.split(formatted_filebase)
        return filebase_path

    def _ensure_path(self, filebase):
        path = filebase.split('/')
        folders_added = []
        for sub_path in path:
            folders_added.append(sub_path)
            parent = '/'.join(folders_added)
            self.azure_service.create_directory(self.share, parent)

    @retry_long
    def _copy_key(self, source_bucket, key_name):
        akey = source_bucket.get_key(key_name)
        if akey.get_metadata('total'):
            self.increment_items(int(akey.get_metadata('total')))
            self.bypass_state.increment_items(int(akey.get_metadata('total')))
        else:
            self.valid_total_count = False
        key = source_bucket.get_key(key_name)
        with TmpFile() as tmp_filename:
            file_name = key_name.split('/')[-1]
            key.get_contents_to_filename(tmp_filename)
            self._ensure_path(self.filebase)
            self.azure_service.put_file_from_path(
                self.share,
                self.filebase,
                file_name,
                tmp_filename,
                max_connections=5,
            )

    def close(self):
        self.bypass_state.delete()
