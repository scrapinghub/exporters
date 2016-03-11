import re
import warnings
from collections import Counter
from exporters.default_retries import retry_long
from exporters.writers.base_writer import BaseWriter


class AzureBlobWriter(BaseWriter):
    """
    Writes items to azure blob containers.

        - account_name (str)
            Public acces name of the azure account.

        - account_key (str)
            Public acces key to the azure account.

        - container (str)
            Blob container name.
    """
    supported_options = {
        'account_name': {'type': basestring, 'env_fallback': 'EXPORTERS_AZUREWRITER_NAME'},
        'account_key': {'type': basestring, 'env_fallback': 'EXPORTERS_AZUREWRITER_KEY'},
        'container': {'type': basestring}
    }
    VALID_CONTAINER_NAME_RE = r'[a-zA-Z0-9-]{3,63}'

    def __init__(self, options, *args, **kw):
        from azure.storage.blob import BlobService
        super(AzureBlobWriter, self).__init__(options, *args, **kw)
        account_name = self.read_option('account_name')
        account_key = self.read_option('account_key')

        self.container = self.read_option('container')
        if '--' in self.container or not re.match(self.VALID_CONTAINER_NAME_RE, self.container):
            help_url = ('https://azure.microsoft.com/en-us/documentation'
                        '/articles/storage-python-how-to-use-blob-storage/')
            warnings.warn("Container name %s doesn't conform with naming rules (see: %s)"
                          % (self.container, help_url))

        self.azure_service = BlobService(account_name, account_key)
        self.azure_service.create_container(self.container)
        self.logger.info('AzureBlobWriter has been initiated.'
                         'Writing to container {}'.format(self.container))
        self.writer_metadata['files_counter'] = Counter()

    def write(self, dump_path, group_key=None):
        self.logger.info('Start uploading {} to {}'.format(dump_path, self.container))
        self._write_blob(dump_path)
        self.writer_metadata['files_counter'][''] += 1
        self.logger.info('Saved {}'.format(dump_path))

    @retry_long
    def _write_blob(self, dump_path):
        self.azure_service.put_block_blob_from_path(
            self.read_option('container'),
            dump_path.split('/')[-1],
            dump_path,
            max_connections=5,
        )
