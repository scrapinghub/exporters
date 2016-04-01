import datetime
import logging
import os
import shutil

from exporters.bypasses.base_bypass import BaseBypass
from exporters.bypasses.s3_bypass_state import S3BypassState
from exporters.default_retries import retry_long
from exporters.readers.s3_reader import get_bucket, S3Reader
from exporters.utils import TmpFile
from exporters.writers.azure_blob_writer import AzureBlobWriter


class AzureBlobS3Bypass(BaseBypass):
    """
    Bypass executed by default when data source is an S3 bucket and data destination is
    an Azure blob container.
    It should be transparent to user. Conditions are:

        - S3Reader and AzureBlobWriter are used on configuration.
        - No filter modules are set up.
        - No transform module is set up.
        - No grouper module is set up.
        - AzureBlobWriter has not a items_limit set in configuration.
        - AzureBlobWriter has default items_per_buffer_write and size_per_buffer_write per default.
    """

    replace_modules = {
        'reader': S3Reader,
        'writer': AzureBlobWriter
    }

    def __init__(self, config, metadata):
        super(AzureBlobS3Bypass, self).__init__(config, metadata)
        self.tmp_folder = None
        self.bypass_state = None
        self.logger = logging.getLogger('bypass_logger')
        self.logger.setLevel(logging.INFO)

    def _handle_conditions_not_met(self, reason):
        self.logger.warning('Skipping Azure file copy optimization bypass because of %s' % reason)
        return False

    def _get_filebase(self, writer_options):
        dest_filebase = writer_options['filebase'].format(datetime.datetime.now())
        dest_filebase = datetime.datetime.now().strftime(dest_filebase)
        return dest_filebase

    def _fill_config_with_env(self):
        reader_opts = self.config.reader_options['options']
        if 'aws_access_key_id' not in reader_opts:
            reader_opts['aws_access_key_id'] = os.environ.get('EXPORTERS_S3READER_AWS_KEY')
        if 'aws_secret_access_key' not in reader_opts:
            reader_opts['aws_secret_access_key'] = os.environ.get('EXPORTERS_S3READER_AWS_SECRET')

    def bypass(self):
        from azure.storage.blob import BlobService
        from copy import deepcopy
        reader_aws_key = self.read_reader_option('aws_access_key_id')
        reader_aws_secret = self.read_reader_option('aws_secret_access_key')
        reader_bucket = self.read_reader_option('bucket')
        self._fill_config_with_env()
        self.bypass_state = S3BypassState(self.config, self.metadata)
        self.total_items = self.bypass_state.stats['total_count']
        self.container = self.read_writer_option('container')
        self.azure_service = BlobService(
            self.read_writer_option('account_name'), self.read_writer_option('account_key'))
        source_bucket = get_bucket(reader_bucket, reader_aws_key, reader_aws_secret)
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
            key.get_contents_to_filename(tmp_filename)
            blob_name = key_name.split('/')[-1]
            self.azure_service.put_block_blob_from_path(
                self.container,
                blob_name,
                tmp_filename,
                max_connections=5,
            )

    def close(self):
        self.bypass_state.delete()
