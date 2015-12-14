import datetime
import logging
import os
import re
import shutil
import tempfile
import uuid
from boto.exception import S3ResponseError
from exporters.default_retries import retry_long
from exporters.module_loader import ModuleLoader


class RequisitesNotMet(Exception):
    """
    Exception thrown when bypass requisites are note meet.
    """


class BaseBypass(object):
    def __init__(self, config):
        self.config = config

    def meets_conditions(self):
        raise NotImplementedError

    def bypass(self):
        raise NotImplementedError


class S3Bypass(BaseBypass):
    """
    Bypass executed when data source and data destination are S3 buckets.
    """

    def __init__(self, config):
        super(S3Bypass, self).__init__(config)
        self.copy_mode = True
        self.tmp_folder = None

    def meets_conditions(self):
        self.config.reader_options['name'].endswith('S3Reader')
        if not self.config.reader_options['name'].endswith('S3Reader') or not self.config.writer_options['name'].endswith('S3Writer'):
            raise RequisitesNotMet
        if not self.config.filter_before_options['name'].endswith('NoFilter'):
            raise RequisitesNotMet
        if not self.config.filter_after_options['name'].endswith('NoFilter'):
            raise RequisitesNotMet
        if not self.config.transform_options['name'].endswith('NoTransform'):
            raise RequisitesNotMet

    def bypass(self):
        import boto
        reader_options = self.config.reader_options['options']
        self.module_loader = ModuleLoader()
        persistence = self.module_loader.load_persistence(self.config.persistence_options)
        writer_options = self.config.writer_options['options']
        source_connection = boto.connect_s3(reader_options['aws_access_key_id'],reader_options['aws_secret_access_key'])
        source_bucket_name = reader_options['bucket']
        source_bucket = source_connection.get_bucket(source_bucket_name)
        self.prefix = reader_options.get('prefix', '')
        self.pattern = reader_options.get('pattern', None)
        dest_connection = boto.connect_s3(writer_options['aws_access_key_id'], writer_options['aws_secret_access_key'])
        dest_bucket_name = writer_options['bucket']
        dest_bucket = dest_connection.get_bucket(dest_bucket_name)
        dest_filebase = writer_options['filebase'].format(datetime.datetime.now())

        position = persistence.get_last_position()
        if not position:
            self.keys = self._get_keys_from_bucket(source_bucket)
            position = {'pending': self.keys, 'done': []}
            persistence.commit_position(position)
        else:
            self.keys = position['pending']

        try:
            for key in self.keys:
                dest_key_name = '{}/{}'.format(dest_filebase, key.split('/')[-1])
                self._copy_key(dest_bucket, dest_key_name, source_bucket, key)
                position['pending'].remove(key)
                position['done'].append(key)
                persistence.commit_position(position)
                logging.log(logging.INFO,
                            'Copied key {} to dest: s3://{}/{}'.format(key, dest_bucket_name, dest_key_name))
        finally:
            if self.tmp_folder:
                shutil.rmtree(self.tmp_folder)

    def _should_add_key(self, key):
        if re.match(os.path.join(self.prefix, self.pattern), key.name):
            return True
        return False

    def _get_keys_from_bucket(self, source_bucket):
        keys = []
        for key in source_bucket.list(prefix=self.prefix):
            if self.pattern:
                if self._should_add_key(key):
                    keys.append(key.name)
            else:
                keys.append(key.name)
        return keys

    def _copy_with_permissions(self, dest_bucket, dest_key_name, source_bucket, key_name):
        try:
            dest_bucket.copy_key(dest_key_name, source_bucket.name, key_name)
        except S3ResponseError:
            logging.log(logging.WARNING, 'No direct copy supported.')
            self.copy_mode = False
            self.tmp_folder = tempfile.mkdtemp()

    def _copy_without_permissions(self, dest_bucket, dest_key_name, source_bucket, key_name):
        key = source_bucket.get_key(key_name)
        tmp_filename = os.path.join(self.tmp_folder, str(uuid.uuid4()))
        key.get_contents_to_filename(tmp_filename)
        dest_key = dest_bucket.new_key(dest_key_name)
        dest_key.set_contents_from_filename(tmp_filename)
        os.remove(tmp_filename)

    @retry_long
    def _copy_key(self, dest_bucket, dest_key_name, source_bucket, key_name):
        if self.copy_mode:
            self._copy_with_permissions(dest_bucket, dest_key_name, source_bucket, key_name)
        if not self.copy_mode:
            self._copy_without_permissions(dest_bucket, dest_key_name, source_bucket, key_name)


