from contextlib import closing
import datetime
import gzip
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
        'bucket': {'type': basestring},
        'aws_access_key_id': {'type': basestring, 'env_fallback': 'EXPORTERS_S3WRITER_AWS_LOGIN'},
        'aws_secret_access_key': {'type': basestring, 'env_fallback': 'EXPORTERS_S3WRITER_AWS_SECRET'},
        'aws_region': {'type': basestring, 'default': None},
        'save_pointer': {'type': basestring, 'default': None},
        'save_metadata': {'type': bool, 'default': True, 'required': False}
    }

    def __init__(self, options):
        import boto

        super(S3Writer, self).__init__(options)
        access_key = self.read_option('aws_access_key_id')
        secret_key = self.read_option('aws_secret_access_key')
        self.aws_region = self.read_option('aws_region')
        bucket_name = self.read_option('bucket')
        self.logger.info('Starting S3Writer for bucket: %s' % bucket_name)

        if self.aws_region is None:
            try:
                self.aws_region = self._get_bucket_location(access_key, secret_key,
                                                            bucket_name)
            except:
                self.aws_region = DEFAULT_BUCKET_REGION

        self.conn = boto.s3.connect_to_region(self.aws_region,
                                              aws_access_key_id=access_key,
                                              aws_secret_access_key=secret_key)
        self.bucket = self.conn.get_bucket(bucket_name)
        self.save_metadata = self.read_option('save_metadata')
        self.logger.info('S3Writer has been initiated.'
                         'Writing to s3://{}/{}'.format(self.bucket.name, self.filebase))

    def _get_bucket_location(self, access_key, secret_key, bucket):
        import boto
        return boto.connect_s3(access_key, secret_key).get_bucket(bucket).get_location() or DEFAULT_BUCKET_REGION

    def _get_total_count(self, dump_path):
        with gzip.open(dump_path) as f:
            total_lines = sum(1 for _ in f)
            return total_lines

    @retry_long
    def _write_s3_key(self, dump_path, key_name):
        destination = 's3://{}/{}'.format(self.bucket.name, key_name)
        self.logger.info('Start uploading {} to {}'.format(dump_path, destination))
        with closing(self.bucket.new_key(key_name)) as key, open(dump_path, 'r') as f:
            if self.save_metadata:
                key.set_metadata('total', self._get_total_count(dump_path))
            progress = BotoDownloadProgress(self.logger)
            key.set_contents_from_file(f, cb=progress)
        self.logger.info('Saved {}'.format(destination))

    def write(self, dump_path, group_key=None):
        if group_key is None:
            group_key = []
        filebase_path, filename = self.create_filebase_name(group_key)
        key_name = filebase_path + '/' + filename
        self._write_s3_key(dump_path, key_name)

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
        self.write_buffer.close()
        self._check_write_consistency()
        if self.read_option('save_pointer'):
            self._update_last_pointer()
