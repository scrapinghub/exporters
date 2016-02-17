from collections import Counter
from contextlib import closing

import datetime

from exporters.default_retries import retry_long
from exporters.writers.filebase_base_writer import FilebaseBaseWriter


class AzureFileWriter(FilebaseBaseWriter):
    """
    Writes items to S3 bucket. It is a File Based writer, so it has filebase
    option available

        - account_name (str)
            Public acces name of the azure account.

        - account_key (str)
            Public acces key to the azure account.

        - share (str)
            File share name.

        - filebase (str)
            Base path to store the items in the share.

    """
    supported_options = {
        'account_name': {'type': basestring, 'env_fallback': 'EXPORTERS_AZUREWRITER_NAME'},
        'account_key': {'type': basestring, 'env_fallback': 'EXPORTERS_AZUREWRITER_KEY'},
        'share': {'type': basestring}
    }

    def __init__(self, options):
        from azure.storage.file import FileService
        super(AzureFileWriter, self).__init__(options)
        account_name = self.read_option('account_name')
        account_key = self.read_option('account_key')
        self.azure_service = FileService(account_name, account_key)
        self.share = self.read_option('share')
        self.azure_service.create_share(self.share)
        self.logger.info('AzureWriter has been initiated.'
                         'Writing to share {}'.format(self.share))
        self.writer_metadata['files_counter'] = Counter()

    def write(self, dump_path, group_key=None):
        if group_key is None:
            group_key = []
        self._write_file(dump_path, group_key)

    def _ensure_path(self, filebase):
        self.azure_service.create_directory(
            self.share,
            filebase,
        )

    @retry_long
    def _write_file(self, dump_path, group_key):
        filebase_path, filename = self.create_filebase_name(group_key)
        key_name = filebase_path + '/' + filename
        self._ensure_path(filebase_path)
        self.azure_service.put_file_from_path(
            self.share,
            filebase_path,
            key_name,
            dump_path,
            max_connections=5,
        )
        self.writer_metadata['files_counter'][filebase_path] += 1

    def close(self):
        """
        Called to clean all possible tmp files created during the process.
        """
        self.write_buffer.close()
        self._check_write_consistency()

    def get_file_suffix(self, path, prefix):
        number_of_keys = self.writer_metadata['files_counter'].get(path, 0)
        suffix = '{}'.format(str(number_of_keys))
        self.writer_metadata['files_counter'][path] = number_of_keys + 1
        return suffix
