from exporters.default_retries import retry_long
from .base_s3_bypass import BaseS3Bypass

S3_URL_EXPIRES_IN = 1800  # half an hour should be enough


class S3AzureBlobBypass(BaseS3Bypass):
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
        super(S3AzureBlobBypass, self).__init__(config, metadata)
        self.container = self.read_option('writer', 'container')
        from azure.storage.blob import BlockBlobService
        self.azure_service = BlockBlobService(
            self.read_option('writer', 'account_name'),
            self.read_option('writer', 'account_key'))

    @classmethod
    def meets_conditions(cls, config):
        if not config.writer_options['name'].endswith('AzureBlobWriter'):
            return cls._handle_conditions_not_met('Wrong reader configured')
        return super(S3AzureBlobBypass, cls).meets_conditions(config)

    @retry_long
    def _copy_s3_key(self, key):
        blob_name = key.name.split('/')[-1]
        self.azure_service.copy_blob(
            self.container,
            blob_name,
            key.generate_url(S3_URL_EXPIRES_IN)
        )
