import os
import re
import boto
import datetime
from retrying import retry
from exporters.writers.base_writer import BaseWriter
import uuid


class S3Writer(BaseWriter):
    """
    Writes items to S3 bucket.

    Needed parameters:

        - bucket (str)
            Name of the bucket to write the items to.

        - aws_access_key_id (str)
            Public acces key to the s3 bucket.

        - aws_secret_access_key (str)
            Secret access key to the s3 bucket.

        - filebase (str)
            Base path to store the items in the bucket.
    """
    requirements = {
        'bucket': {'type': basestring, 'required': True},
        'aws_access_key_id': {'type': basestring, 'required': True},
        'aws_secret_access_key': {'type': basestring, 'required': True},
        'filebase': {'type': basestring, 'required': True}
    }

    def __init__(self, options, settings):
        super(S3Writer, self).__init__(options, settings)
        self.conn = boto.connect_s3(self.read_option('aws_access_key_id'), self.read_option('aws_secret_access_key'))
        self.bucket = self.conn.get_bucket(self.read_option('bucket'))
        self.filebase = self.read_option('filebase').format(datetime.datetime.now())
        self.logger.info('S3Writer has been initiated. Writing to s3://{}{}'.format(self.bucket, self.filebase))

    @retry(wait_exponential_multiplier=500, wait_exponential_max=10000, stop_max_attempt_number=10)
    def write(self, dump_path, group_key=None):
        if group_key is None:
            group_key = []
        normalized = [re.sub('\W', '_', s) for s in group_key]
        destination_path = os.path.join(self.filebase, os.path.sep.join(normalized))
        key_name = '{}/{}_{}.{}'.format(destination_path, 'ds_dump', uuid.uuid4(), 'gz')
        key = self.bucket.new_key(key_name)
        self.logger.debug('Uploading predump file')
        with open(dump_path, 'r') as f:
            key.set_contents_from_file(f)
        key.close()
        self.logger.debug('Flushed {} to s3://{}/{}'.format(dump_path, self.read_option('bucket'), key_name))
