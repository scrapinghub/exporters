from collections import Counter

from azure.common import AzureMissingResourceHttpError

from exporters.default_retries import retry_long
from exporters.writers.base_writer import InconsistentWriteState
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

    def __init__(self, options, *args, **kw):
        from azure.storage.file import FileService
        super(AzureFileWriter, self).__init__(options, *args, **kw)
        account_name = self.read_option('account_name')
        account_key = self.read_option('account_key')
        self.azure_service = FileService(account_name, account_key)
        self.share = self.read_option('share')
        self.azure_service.create_share(self.share)
        self.logger.info('AzureWriter has been initiated.'
                         'Writing to share {}'.format(self.share))
        self.writer_metadata['files_counter'] = Counter()
        self.writer_metadata['files_written'] = []

    def _update_metadata(self, dump_path, filebase_path, file_name):
        buffer_info = self.write_buffer.metadata[dump_path]
        file_info = {
            'file_name': file_name,
            'filebase_path': filebase_path,
            'size': buffer_info['size'],
            'number_of_records': buffer_info['number_of_records']
        }
        self.writer_metadata['files_written'].append(file_info)

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
        self._update_metadata(dump_path, filebase_path, filename)
        self.writer_metadata['files_counter'][filebase_path] += 1

    def get_file_suffix(self, path, prefix):
        number_of_keys = self.writer_metadata['files_counter'].get(path, 0)
        suffix = '{}'.format(str(number_of_keys))
        self.writer_metadata['files_counter'][path] = number_of_keys + 1
        return suffix

    def _check_write_consistency(self):
        for file_info in self.writer_metadata['files_written']:
            try:
                file_properties = self.azure_service.get_file_properties(self.share, file_info['filebase_path'], file_info['file_name'])
                file_size = file_properties.get('content-length')
                if str(file_size) != str(file_info['size']):
                    raise InconsistentWriteState('File {} has wrong size. Extected: {} - got {}'.format(
                                file_info['file_name'], file_info['size'], file_size))
            except AzureMissingResourceHttpError:
                raise InconsistentWriteState('Missing file {}'.format(file_info['file_name']))
        self.logger.info('Consistency check passed')