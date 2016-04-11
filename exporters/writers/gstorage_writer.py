import json
import os
import six

from base64 import b64encode
from binascii import unhexlify

from exporters.default_retries import retry_long
from exporters.writers.filebase_base_writer import FilebaseBaseWriter
from exporters.utils import TemporaryDirectory
from exporters.writers.base_writer import InconsistentWriteState


class GStorageWriter(FilebaseBaseWriter):

    """
    Writes items to Google Storage buckets. It is a File Based writer, so it has filebase
    option available

        - filebase (str)
            Path to store the exported files

        - project (str)
            Valid project name

        - bucket (str)
            Google Storage bucket name

        - credentials (dict)
            Object with valid google credentials
    """

    supported_options = {
        'project': {'type': six.string_types},
        'bucket': {'type': six.string_types},
        'credentials': {'type': dict}
    }

    def __init__(self, options, *args, **kwargs):
        from gcloud import storage
        super(GStorageWriter, self).__init__(options, *args, **kwargs)
        project = self.read_option('project')
        bucket_name = self.read_option('bucket')

        with TemporaryDirectory() as temp_dir:
            credentials_file = os.path.join(temp_dir, 'credentials.json')
            with open(credentials_file, 'w') as f:
                f.write(json.dumps(self.read_option('credentials')))
            client = storage.Client.from_service_account_json(credentials_file,
                                                              project=project)
        self.bucket = client.bucket(bucket_name)
        self.logger.info('GStorageWriter has been initiated.'
                         'Writing to {}'.format(self._blob_url(bucket_name, self.filebase)))
        self.set_metadata('files_written', [])

    def _blob_url(self, bucket_name, blob_name):
        return 'https://storage.cloud.google.com/{}/{}'.format(bucket_name, blob_name)

    @retry_long
    def _write_gstorage_blob(self, dump_path, blob_name):
        destination = self._blob_url(self.bucket.name, blob_name)
        self.logger.info('Start uploading {} to {}'.format(dump_path, destination))

        with open(dump_path, 'r') as f:
            blob = self.bucket.blob(blob_name)
            blob.upload_from_file(f)

        self._update_metadata(dump_path, blob)
        self.logger.info('Saved {}'.format(destination))

    def write(self, dump_path, group_key=None, file_name=None):
        if group_key is None:
            group_key = []

        filebase_path, file_name = self.create_filebase_name(group_key, file_name=file_name)
        blob_name = filebase_path + '/' + file_name
        self._write_gstorage_blob(dump_path, blob_name)
        self.last_written_file = blob_name

    def _update_metadata(self, dump_path, blob):
        buffer_info = self.write_buffer.metadata[dump_path]
        key_info = {
            'size': buffer_info['size'],
            'remote_size': blob.size,
            'hash': b64encode(unhexlify(buffer_info['compressed_hash'])),
            'remote_hash': blob.md5_hash,
            'title': blob.name,
        }
        print key_info
        self.get_metadata('files_written').append(key_info)

    def _check_write_consistency(self):
        for file_info in self.get_metadata('files_written'):
            if file_info['size'] != file_info['remote_size']:
                raise InconsistentWriteState(('Unexpected size of file {title}.'
                    'expected {size} - got {remote_size}').format(**file_info))
            if file_info['hash'] != file_info['remote_hash']:
                raise InconsistentWriteState(('Unexpected hash of file {title}.'
                    'expected {hash} - got {remote_hash}').format(**file_info))
