import os
from collections import Counter
from contextlib import closing, contextmanager
import six
from exporters.default_retries import retry_long
from exporters.progress_callback import BotoDownloadProgress
from exporters.utils import CHUNK_SIZE, split_file, calculate_multipart_etag, get_bucket_name
from exporters.writers.base_writer import InconsistentWriteState
from exporters.writers.filebase_base_writer import FilebaseBaseWriter


DEFAULT_BUCKET_REGION = 'us-east-1'


@contextmanager
def multipart_upload(bucket, key_name):
    mp = bucket.initiate_multipart_upload(key_name)
    try:
        yield mp
        mp.complete_upload()
    except:
        mp.cancel_upload()
        raise


def should_use_multipart_upload(path, bucket):
    from boto.exception import S3ResponseError
    # We need to check if we have READ permissions on this bucket, as they are
    # needed to perform the complete_upload operation.
    try:
        acl = bucket.get_acl()
        for grant in acl.acl.grants:
            if grant.permission == 'READ':
                break
    except S3ResponseError:
        return False
    return os.path.getsize(path) > CHUNK_SIZE


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
        bucket_name = get_bucket_name(self.read_option('bucket'))
        self.logger.info('Starting S3Writer for bucket: %s' % bucket_name)

        if self.aws_region is None:
            self.aws_region = self._get_bucket_location(access_key, secret_key,
                                                        bucket_name)

        self.conn = boto.s3.connect_to_region(self.aws_region,
                                              aws_access_key_id=access_key,
                                              aws_secret_access_key=secret_key)
        self.bucket = self.conn.get_bucket(bucket_name, validate=False)
        self.save_metadata = self.read_option('save_metadata')
        self.set_metadata('files_counter', Counter())
        self.set_metadata('keys_written', [])

    def _get_bucket_location(self, access_key, secret_key, bucket):
        import boto
        try:
            conn = boto.connect_s3(access_key, secret_key)
            return conn.get_bucket(bucket).get_location() or DEFAULT_BUCKET_REGION
        except:
            return DEFAULT_BUCKET_REGION

    def _update_metadata(self, dump_path, key_name):
        buffer_info = self.write_buffer.metadata[dump_path]
        key_info = {
            'key_name': key_name,
            'size': buffer_info['size'],
            'number_of_records': buffer_info['number_of_records']
        }
        keys_written = self.get_metadata('keys_written')
        keys_written.append(key_info)
        self.set_metadata('keys_written', keys_written)

    def _get_total_count(self, dump_path):
        return self.write_buffer.get_metadata(dump_path, 'number_of_records') or 0

    def _ensure_proper_key_permissions(self, key):
        from boto.exception import S3ResponseError
        try:
            key.set_acl('bucket-owner-full-control')
        except S3ResponseError:
            self.logger.warning('We have no READ_ACP/WRITE_ACP permissions')

    def _set_key_metadata(self, key, metadata):
        from boto.exception import S3ResponseError
        try:
            for name, value in metadata.iteritems():
                key.set_metadata(name, value)
        except S3ResponseError:
            self.logger.warning(
                    'We have no READ_ACP/WRITE_ACP permissions, '
                    'so we could not add metadata info')

    def _save_metadata_for_key(self, key, dump_path, md5=None):
        from boto.exception import S3ResponseError
        from boto.utils import compute_md5
        try:
            key.set_metadata('total', self._get_total_count(dump_path))
            if md5:
                key.set_metadata('md5', md5)
            else:
                with open(dump_path, 'r') as f:
                    key.set_metadata('md5', compute_md5(f))
        except S3ResponseError:
            self.logger.warning(
                    'We have no READ_ACP/WRITE_ACP permissions, '
                    'so we could not add metadata info')

    def _upload_small_file(self, dump_path, key_name):
        with closing(self.bucket.new_key(key_name)) as key, open(dump_path, 'r') as f:
            buffer_info = self.write_buffer.metadata[dump_path]
            md5 = key.get_md5_from_hexdigest(buffer_info['file_hash'])
            if self.save_metadata:
                self._save_metadata_for_key(key, dump_path, md5)
            progress = BotoDownloadProgress(self.logger)
            key.set_contents_from_file(f, cb=progress, md5=md5)
            self._ensure_proper_key_permissions(key)

    @retry_long
    def _upload_chunk(self, mp, chunk):
        mp.upload_part_from_file(chunk.bytes, part_num=chunk.number)

    def _upload_large_file(self, dump_path, key_name):
        self.logger.debug('Using multipart S3 uploader')
        with multipart_upload(self.bucket, key_name) as mp:
            for chunk in split_file(dump_path):
                self._upload_chunk(mp, chunk)
                self.logger.debug(
                        'Uploaded chunk number {}'.format(chunk.number))
        with closing(self.bucket.get_key(key_name)) as key:
            self._ensure_proper_key_permissions(key)
            if self.save_metadata:
                md5 = calculate_multipart_etag(dump_path, CHUNK_SIZE)
                self._save_metadata_for_key(key, dump_path, md5=md5)

    def _write_s3_key(self, dump_path, key_name):
        destination = 's3://{}/{}'.format(self.bucket.name, key_name)
        self.logger.info('Start uploading {} to {}'.format(dump_path, destination))
        if should_use_multipart_upload(dump_path, self.bucket):
            self._upload_large_file(dump_path, key_name)
        else:
            self._upload_small_file(dump_path, key_name)
        self.last_written_file = destination
        self.logger.info('Saved {}'.format(destination))

    def write(self, dump_path, group_key=None, file_name=None):
        if group_key is None:
            group_key = []
        filebase_path, file_name = self.create_filebase_name(group_key, file_name=file_name)
        key_name = filebase_path + '/' + file_name
        self._write_s3_key(dump_path, key_name)
        self._update_metadata(dump_path, key_name)
        self.get_metadata('files_counter')[filebase_path] += 1

    @retry_long
    def _write_s3_pointer(self, save_pointer, filebase):
        with closing(self.bucket.new_key(save_pointer)) as key:
            key.set_contents_from_string(filebase)

    def _update_last_pointer(self):
        save_pointer = self.read_option('save_pointer')
        self._write_s3_pointer(save_pointer, self.filebase.dirname_template + '/')

    def close(self):
        """
        Called to clean all possible tmp files created during the process.
        """
        if self.read_option('save_pointer'):
            self._update_last_pointer()
        super(S3Writer, self).close()

    def get_file_suffix(self, path, prefix):
        number_of_keys = self.get_metadata('files_counter').get(path, 0)
        suffix = '{}'.format(str(number_of_keys))
        return suffix

    def _check_write_consistency(self):
        from boto.exception import S3ResponseError
        for key_info in self.get_metadata('keys_written'):
            try:
                key = self.bucket.get_key(key_info['key_name'])
                if not key:
                    raise InconsistentWriteState('Key {} not found in bucket'.format(
                        key_info['key_name']))
                if str(key.content_length) != str(key_info['size']):
                    raise InconsistentWriteState(
                        'Key {} has unexpected size. (expected {} - got {})'.format(
                            key_info['key_name'], key_info['size'], key.content_length))
                if self.save_metadata:
                    if str(key.get_metadata('total')) != str(key_info['number_of_records']):
                        raise InconsistentWriteState(
                            'Unexpected number of records for key {}. ('
                            'expected {} - got {})'.format(key_info['key_name'],
                                                           key_info['number_of_records'],
                                                           key.get_metadata('total')))
            except S3ResponseError:
                self.logger.warning(
                    'Skipping consistency check for key {}. Probably due to lack of '
                    'read permissions'.format(key_info['key_name']))
        self.logger.info('Consistency check passed')
