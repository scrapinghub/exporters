from collections import Counter
from contextlib import closing

import datetime

from exporters.default_retries import retry_long
from exporters.writers.filebase_base_writer import FilebaseBaseWriter


class AzureWriter(FilebaseBaseWriter):
    """
    Writes items to S3 bucket. It is a File Based writer, so it has filebase
    option available

        - bucket (str)
            Name of the bucket to write the items to.

        - aws_access_key_id (str)
            Public acces key to the s3 bucket.

        - aws_secret_access_key (str)
            Secret access key to the s3 bucket.

        - filebase (str)
            Base path to store the items in the bucket.

        - aws_region (str)
            AWS region to connect to.

        - save_metadata (bool)
            Save key's items count as metadata. Default: True

        - filebase
            Path to store the exported files
    """
    supported_options = {
        'account_name': {'type': basestring, 'env_fallback': 'EXPORTERS_AZUREWRITER_NAME'},
        'account_key': {'type': basestring, 'env_fallback': 'EXPORTERS_AZUREWRITER_KEY'},
        'output_type': {'type': basestring, 'default': 'blob'},
        'share': {'type': basestring, 'default': None}
    }

    def __init__(self, options):
        from azure.storage.blob import BlobService
        from azure.storage.file import FileService
        super(AzureWriter, self).__init__(options)
        account_name = self.read_option('account_name')
        account_key = self.read_option('account_key')
        self.output_type = self.read_option('output_type')
        self.azure_service = None
        self.filebase = self.read_option('filebase')
        self.filebase = self.filebase.format(date=datetime.datetime.now())
        self.filebase = datetime.datetime.now().strftime(self.filebase)
        if self.output_type == 'blob':
            self.azure_service = BlobService(account_name, account_key)
            self.azure_service.create_container()
        elif self.output_type == 'file':
            self.azure_service = FileService(account_name, account_key)
            self.azure_service.create_share(self.read_option('share'))
        else:
            raise ValueError('Wrong output type setting. It should be one of [\'blob\', \'file\']')

        self.logger.info('AzureWriter has been initiated.'
                         'Writing to filebase {}'.format(self.bucket.name, self.filebase))
        self.writer_metadata['files_counter'] = Counter()

    def write(self, dump_path, group_key=None):
        if group_key is None:
            group_key = []
        if self.output_type == 'blob':
            self._write_blob(dump_path)
        else:
            self._write_file(dump_path, group_key)

        # self.writer_metadata['files_counter'][filebase_path] += 1

    @retry_long
    def _write_blob(self, dump_path):
        self.azure_service.put_block_blob_from_path(
            self.read_option('container'),
            dump_path.split('/')[-1],
            dump_path,
            max_connections=5,
        )

    @retry_long
    def _write_file(self, dump_path, group_key):

        filebase_path, filename = self.create_filebase_name(group_key)
        key_name = filebase_path + '/' + filename
        self._ensure_path(filebase_path)

        self.azure_service.put_file_from_path(
            'myshare',
                    'uploads',
            'image.png',
            'localimage.png',
            max_connections=5,
        )



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
