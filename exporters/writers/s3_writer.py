from collections import Counter
from contextlib import closing
import datetime
import six
from exporters.default_retries import retry_long
from exporters.progress_callback import BotoDownloadProgress
from exporters.writers.filebase_base_writer import FilebaseBaseWriter

DEFAULT_BUCKET_REGION = 'us-east-1'


class S3Writer(FilebaseBaseWriter):
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
        'bucket': {'type': six.string_types},
        'aws_access_key_id': {
            'type': six.string_types,
            'env_fallback': 'EXPORTERS_S3WRITER_AWS_LOGIN'
        },
        'aws_secret_access_key': {
            'type': six.string_types,
            'env_fallback': 'EXPORTERS_S3WRITER_AWS_SECRET'
        },
        'aws_region': {'type': six.string_types, 'default': None},
        'save_pointer': {'type': six.string_types, 'default': None},
        'save_metadata': {'type': bool, 'default': True, 'required': False}
    }

    def __init__(self, options, *args, **kwargs):
        import boto

        super(S3Writer, self).__init__(options, *args, **kwargs)
        access_key = self.read_option('aws_access_key_id')
        secret_key = self.read_option('aws_secret_access_key')
        self.aws_region = self.read_option('aws_region')
        bucket_name = self.read_option('bucket')
        self.logger.info('Starting S3Writer for bucket: %s' % bucket_name)

        if self.aws_region is None:
            self.aws_region = self._get_bucket_location(access_key, secret_key,
                                                        bucket_name)

        self.conn = boto.s3.connect_to_region(self.aws_region,
                                              aws_access_key_id=access_key,
                                              aws_secret_access_key=secret_key)
        self.bucket = self.conn.get_bucket(bucket_name, validate=False)
        self.save_metadata = self.read_option('save_metadata')
        self.logger.info('S3Writer has been initiated.'
                         'Writing to s3://{}/{}'.format(self.bucket.name, self.filebase))
        self.set_metadata('files_counter', Counter())

    def _get_bucket_location(self, access_key, secret_key, bucket):
        import boto
        try:
            conn = boto.connect_s3(access_key, secret_key)
            return conn.get_bucket(bucket).get_location() or DEFAULT_BUCKET_REGION
        except:
            return DEFAULT_BUCKET_REGION

    def _get_total_count(self, dump_path):
        return self.write_buffer.get_metadata(dump_path, 'number_of_records') or 0

    def _ensure_proper_key_permissions(self, key):
        from boto.exception import S3ResponseError
        try:
            key.set_acl('bucket-owner-full-control')
        except S3ResponseError:
            self.logger.warning('We have no READ_ACP/WRITE_ACP permissions')

    @retry_long
    def _write_s3_key(self, dump_path, key_name):
        from boto.utils import compute_md5
        destination = 's3://{}/{}'.format(self.bucket.name, key_name)
        self.logger.info('Start uploading {} to {}'.format(dump_path, destination))
        with closing(self.bucket.new_key(key_name)) as key, open(dump_path, 'r') as f:
            md5 = compute_md5(f)
            if self.save_metadata:
                key.set_metadata('total', self._get_total_count(dump_path))
                key.set_metadata('md5', md5)
            progress = BotoDownloadProgress(self.logger)
            key.set_contents_from_file(f, cb=progress, md5=md5)
            self._ensure_proper_key_permissions(key)
        self.last_written_file = destination
        self.logger.info('Saved {}'.format(destination))

    def write(self, dump_path, group_key=None, file_name=None):
        if group_key is None:
            group_key = []
        filebase_path, file_name = self.create_filebase_name(group_key, file_name=file_name)
        key_name = filebase_path + '/' + file_name
        self._write_s3_key(dump_path, key_name)
        self.get_metadata('files_counter')[filebase_path] += 1

    @retry_long
    def _write_s3_pointer(self, save_pointer, filebase):
        with closing(self.bucket.new_key(save_pointer)) as key:
            key.set_contents_from_string(filebase)

    def _update_last_pointer(self):
        save_pointer = self.read_option('save_pointer')
        filebase = self.read_option('filebase')
        filebase = filebase.format(date=datetime.datetime.now())
        filebase = datetime.datetime.now().strftime(filebase)
        self._write_s3_pointer(save_pointer, filebase)

    def close(self):
        """
        Called to clean all possible tmp files created during the process.
        """
        if self.write_buffer is not None:
            self.write_buffer.close()
        self._check_write_consistency()
        if self.read_option('save_pointer'):
            self._update_last_pointer()
        super(S3Writer, self).close()

    def get_file_suffix(self, path, prefix):
        number_of_keys = self.get_metadata('files_counter').get(path, 0)
        suffix = '{}'.format(str(number_of_keys))
        return suffix
