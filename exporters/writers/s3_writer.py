import os
import re
import datetime
from retrying import retry
import uuid
from exporters.writers.filebase_base_writer import FilebaseBaseWriter


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
    """
    supported_options = {
        'bucket': {'type': basestring},
        'aws_access_key_id': {'type': basestring},
        'aws_secret_access_key': {'type': basestring},
        'aws_region': {'type': basestring, 'default': 'us-east-1'},
    }

    def __init__(self, options):
        import boto
        super(S3Writer, self).__init__(options)
        access_key = self.read_option('aws_access_key_id')
        secret_key = self.read_option('aws_secret_access_key')
        aws_region = self.read_option('aws_region')

        self.conn = boto.s3.connect_to_region(aws_region,
                                              aws_access_key_id=access_key,
                                              aws_secret_access_key=secret_key)
        self.bucket = self.conn.get_bucket(self.read_option('bucket'))
        self.filebase = self.read_option('filebase').format(datetime.datetime.now())
        self.logger.info('S3Writer has been initiated. Writing to s3://{}{}'.format(self.bucket, self.filebase))

    @retry(wait_exponential_multiplier=500, wait_exponential_max=10000, stop_max_attempt_number=10)
    def write(self, dump_path, group_key=None):
        if group_key is None:
            group_key = []
        normalized = [re.sub('\W', '_', s) for s in group_key]
        destination_path = os.path.join(self.filebase_path, os.path.sep.join(normalized))
        key_name = '{}/{}_{}.{}'.format(destination_path, self.prefix, uuid.uuid4(), 'gz')
        key = self.bucket.new_key(key_name)
        self.logger.debug('Uploading dump file')
        with open(dump_path, 'r') as f:
            key.set_contents_from_file(f)
        key.close()
        self.logger.debug('Saved {} to s3://{}/{}'.format(dump_path, self.read_option('bucket'), key_name))
