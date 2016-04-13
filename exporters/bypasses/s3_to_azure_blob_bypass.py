from exporters.default_retries import retry_long
from exporters.export_managers.base_bypass import RequisitesNotMet
from exporters.utils import TmpFile
from .base_s3_bypass import BaseS3Bypass


class AzureBlobS3Bypass(BaseS3Bypass):
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

    def __init__(self, config, metadata):
        super(AzureBlobS3Bypass, self).__init__(config, metadata)
        self.container = self.read_option('writer', 'container')
        from azure.storage.blob import BlobService
        self.azure_service = BlobService(
            self.read_option('writer', 'account_name'),
            self.read_option('writer', 'account_key'))

    @classmethod
    def meets_conditions(cls, config):
        if not config.writer_options['name'].endswith('AzureBlobWriter'):
            raise RequisitesNotMet
        super(AzureBlobS3Bypass, cls).meets_conditions(config)

    @retry_long
    def _copy_s3_key(self, key):
        with TmpFile() as tmp_filename:
            key.get_contents_to_filename(tmp_filename)
            blob_name = key.name.split('/')[-1]
            self.azure_service.put_block_blob_from_path(
                self.container,
                blob_name,
                tmp_filename,
                max_connections=5,
            )
