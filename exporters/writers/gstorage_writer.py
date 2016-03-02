import json
import os

from exporters.default_retries import retry_long
from exporters.writers.filebase_base_writer import FilebaseBaseWriter
from exporters.utils import TemporaryDirectory


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
        'project': {'type': basestring},
        'bucket': {'type': basestring},
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

    def _blob_url(self, bucket_name, blob_name):
        return 'https://storage.cloud.google.com/{}/{}'.format(bucket_name, blob_name)

    @retry_long
    def _write_gstorage_blob(self, dump_path, blob_name):
        destination = self._blob_url(self.bucket.name, blob_name)
        self.logger.info('Start uploading {} to {}'.format(dump_path, destination))

        with open(dump_path, 'r') as f:
            blob = self.bucket.blob(blob_name)
            blob.upload_from_file(f)

        self.logger.info('Saved {}'.format(destination))

    def write(self, dump_path, group_key=None, file_name=None):
        if group_key is None:
            group_key = []

        filebase_path, file_name = self.create_filebase_name(group_key, file_name=file_name)
        blob_name = filebase_path + '/' + file_name
        self._write_gstorage_blob(dump_path, blob_name)
        self.last_written_file = blob_name
