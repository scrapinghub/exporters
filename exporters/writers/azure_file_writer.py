import six
from collections import Counter
from exporters.default_retries import retry_long
from exporters.writers.base_writer import InconsistentWriteState
from exporters.writers.filebase_base_writer import FilebaseBaseWriter


class AzureFileWriter(FilebaseBaseWriter):
    """
    Writes items to azure file shares. It is a File Based writer, so it has filebase
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
        'account_name': {'type': six.string_types, 'env_fallback': 'EXPORTERS_AZUREWRITER_NAME'},
        'account_key': {'type': six.string_types, 'env_fallback': 'EXPORTERS_AZUREWRITER_KEY'},
        'share': {'type': six.string_types}
    }

    def __init__(self, options, meta, *args, **kw):
        from azure.storage.file import FileService
        super(AzureFileWriter, self).__init__(options, meta, *args, **kw)
        account_name = self.read_option('account_name')
        account_key = self.read_option('account_key')
        self.azure_service = FileService(account_name, account_key)
        self.share = self.read_option('share')
        self.azure_service.create_share(self.share)
        self.logger.info('AzureWriter has been initiated.'
                         'Writing to share {}'.format(self.share))
        self.set_metadata('files_counter', Counter())
        self.set_metadata('files_written', [])

    def write(self, dump_path, group_key=None, file_name=None):
        if group_key is None:
            group_key = []
        self._write_file(dump_path, group_key, file_name)

    def _update_metadata(self, dump_path, filebase_path, file_name):
        buffer_info = self.write_buffer.metadata[dump_path]
        file_info = {
            'file_name': file_name,
            'filebase_path': filebase_path,
            'size': buffer_info['size'],
            'number_of_records': buffer_info['number_of_records']
        }
        files_written = self.get_metadata('files_written')
        files_written.append(file_info)
        self.set_metadata('files_written', files_written)
        self.get_metadata('files_counter')[filebase_path] += 1

    def _ensure_path(self, filebase):
        path = filebase.split('/')
        folders_added = []
        for sub_path in path:
            folders_added.append(sub_path)
            parent = '/'.join(folders_added)
            self.azure_service.create_directory(self.share, parent)

    @retry_long
    def _write_file(self, dump_path, group_key, file_name=None):
        filebase_path, file_name = self.create_filebase_name(group_key, file_name=file_name)
        self._ensure_path(filebase_path)
        self.azure_service.create_file_from_path(
            self.share,
            filebase_path,
            file_name,
            dump_path,
            max_connections=5,
        )
        self._update_metadata(dump_path, filebase_path, file_name)

    def get_file_suffix(self, path, prefix):
        number_of_keys = self.get_metadata('files_counter').get(path, 0)
        suffix = '{}'.format(str(number_of_keys))
        return suffix

    def _check_write_consistency(self):
        from azure.common import AzureMissingResourceHttpError
        for file_info in self.get_metadata('files_written'):
            try:
                afile = self.azure_service.get_file_properties(
                    self.share, file_info['filebase_path'], file_info['file_name'])
                file_size = afile.properties.content_length
                if str(file_size) != str(file_info['size']):
                    raise InconsistentWriteState(
                        'File {} has unexpected size. (expected {} - got {})'.format(
                            file_info['file_name'], file_info['size'], file_size)
                    )
            except AzureMissingResourceHttpError:
                raise InconsistentWriteState('Missing file {}'.format(file_info['file_name']))
        self.logger.info('Consistency check passed')
