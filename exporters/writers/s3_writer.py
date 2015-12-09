from contextlib import closing
from exporters.default_retries import retry_long
from exporters.writers.filebase_base_writer import FilebaseBaseWriter

DEFAULT_BUCKET_REGION = 'us-east-1'


class S3Writer(FilebaseBaseWriter):
    """
    Writes items to S3 bucket.

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
    """
    supported_options = {
        'bucket': {'type': basestring},
        'aws_access_key_id': {'type': basestring, 'env_fallback': 'EXPORTERS_S3WRITER_AWS_LOGIN'},
        'aws_secret_access_key': {'type': basestring, 'env_fallback': 'EXPORTERS_S3WRITER_AWS_SECRET'},
        'aws_region': {'type': basestring, 'default': None},
        'save_metadata': {'type': bool, 'default': True, 'required': False}
    }

    def __init__(self, options):
        import boto

        super(S3Writer, self).__init__(options)
        access_key = self.read_option('aws_access_key_id')
        secret_key = self.read_option('aws_secret_access_key')
        self.aws_region = self.read_option('aws_region')
        bucket_name = self.read_option('bucket')

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
        return self.write_buffer.stats['written_keys']['keys'][dump_path]['number_of_records']

    @retry_long
    def _write_s3_key(self, dump_path, key_name):
        destination = 's3://{}/{}'.format(self.bucket.name, key_name)
        self.logger.info('Start uploading {} to {}'.format(dump_path, destination))
        with closing(self.bucket.new_key(key_name)) as key, open(dump_path, 'r') as f:
            if self.save_metadata:
                key.set_metadata('total', self._get_total_count(dump_path))
            key.set_contents_from_file(f)

        self.logger.info('Saved {}'.format(destination))

    def write(self, dump_path, group_key=None):
        if group_key is None:
            group_key = []

        filebase_path, filename = self.create_filebase_name(group_key)
        key_name = filebase_path + '/' + filename
        self._write_s3_key(dump_path, key_name)
