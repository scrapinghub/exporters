from collections import Counter
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
        path = filebase.split('/')
        folders_added = []
        for sub_path in path:
            folders_added.append(sub_path)
            parent = '/'.join(folders_added)
            self.azure_service.create_directory(
                    self.share,
                    parent,
            )

    @retry_long
    def _write_file(self, dump_path, group_key):
        filebase_path, filename = self.create_filebase_name(group_key)
        self._ensure_path(filebase_path)
        self.azure_service.put_file_from_path(
            self.share,
            filebase_path,
            filename,
            dump_path,
            max_connections=5,
        )
        self.writer_metadata['files_counter'][filebase_path] += 1

    def get_file_suffix(self, path, prefix):
        number_of_keys = self.writer_metadata['files_counter'].get(path, 0)
        suffix = '{}'.format(str(number_of_keys))
        self.writer_metadata['files_counter'][path] = number_of_keys + 1
        self.writer_metadata['written_files'].append(suffix)
        return suffix
