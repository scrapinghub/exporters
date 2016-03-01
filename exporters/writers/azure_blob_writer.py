from collections import Counter
from azure.common import AzureMissingResourceHttpError
from exporters.default_retries import retry_long
from exporters.writers.base_writer import BaseWriter, InconsistentWriteState


class AzureBlobWriter(BaseWriter):
    """
    Writes items to S3 bucket. It is a File Based writer, so it has filebase
    option available

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

    def __init__(self, options, *args, **kw):
        from azure.storage.blob import BlobService
        super(AzureBlobWriter, self).__init__(options, *args, **kw)
        account_name = self.read_option('account_name')
        account_key = self.read_option('account_key')
        self.container = self.read_option('container')
        self.azure_service = BlobService(account_name, account_key)
        self.azure_service.create_container(self.container)
        self.logger.info('AzureBlobWriter has been initiated.'
                         'Writing to container {}'.format(self.container))
        self.writer_metadata['files_counter'] = Counter()
        self.writer_metadata['blobs_written'] = []

    def write(self, dump_path, group_key=None):
        self._write_blob(dump_path)
        self.writer_metadata['files_counter'][''] += 1

    @retry_long
    def _write_blob(self, dump_path):
        blob_name = dump_path.split('/')[-1]
        self.azure_service.put_block_blob_from_path(
            self.read_option('container'),
            blob_name,
            dump_path,
            max_connections=5,
        )
        self._update_metadata(dump_path, blob_name)

    def _update_metadata(self, dump_path, blob_name):
        buffer_info = self.write_buffer.metadata[dump_path]
        file_info = {
            'blob_name': blob_name,
            'size': buffer_info['size'],
            'number_of_records': buffer_info['number_of_records']
        }
        self.writer_metadata['blobs_written'].append(file_info)

    def _check_write_consistency(self):
        for blob_info in self.writer_metadata['blobs_written']:
            try:
                blob_properties = self.azure_service.get_blob_properties(self.read_option('container'), blob_info['blob_name'])
                blob_size = blob_properties.get('content-length')
                if str(blob_size) != str(blob_info['size']):
                    raise InconsistentWriteState('File {} has wrong size. Extected: {} - got {}'.format(
                                blob_info['blob_name'], blob_info['size'], blob_size))
            except AzureMissingResourceHttpError:
                raise InconsistentWriteState('Missing blob {}'.format(blob_info['file_name']))
        self.logger.info('Consistency check passed')