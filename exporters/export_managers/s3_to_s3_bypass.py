import datetime
import logging
import os
import re
import shutil
import tempfile
import uuid
from boto.exception import S3ResponseError
from exporters.default_retries import retry_long
from exporters.export_managers.base_bypass import RequisitesNotMet, BaseBypass
from exporters.module_loader import ModuleLoader


def get_bucket(bucket, aws_access_key_id, aws_secret_access_key, **kwargs):
    import boto
    connection = boto.connect_s3(aws_access_key_id, aws_secret_access_key)
    return connection.get_bucket(bucket)


class S3BucketKeysHelper(object):
    def __init__(self, config):
        reader_options = config.reader_options['options']
        self.source_bucket = get_bucket(**reader_options)
        self.prefix = reader_options.get('prefix', '')
        self.pattern = reader_options.get('pattern', None)

    def _get_keys_from_bucket(self):
        keys = []
        for key in self.source_bucket.list(prefix=self.prefix):
            if self.pattern:
                if self._should_add_key(key):
                    keys.append(key.name)
            else:
                keys.append(key.name)
        return keys

    def _should_add_key(self, key):
        if re.match(os.path.join(self.prefix, self.pattern), key.name):
            return True
        return False

    def pending_keys(self):
        return self._get_keys_from_bucket()


class S3BypassState(object):

    def __init__(self, config):
        self.config = config
        module_loader = ModuleLoader()
        self.state = module_loader.load_persistence(config.persistence_options)
        self.state_position = self.state.get_last_position()
        if not self.state_position:
            self.s3_keys = S3BucketKeysHelper(self.config)
            self.pending = self.s3_keys.pending_keys()
            self.done = []
            self.skipped = []
            self.state.commit_position(self._get_state())
        else:
            self.pending = self.state_position['pending']
            self.done = []
            self.skipped = self.state_position['done']
            self.keys = self.pending

    def _get_state(self):
        return {'pending': self.pending, 'done': self.done, 'skipped': self.skipped}

    def commit_copied_key(self, key):
        self.pending.remove(key)
        self.done.append(key)
        self.state.commit_position(self._get_state())

    def pending_keys(self):
        return self.pending


class S3Bypass(BaseBypass):
    """
    Bypass executed when data source and data destination are S3 buckets.
    """

    def __init__(self, config):
        super(S3Bypass, self).__init__(config)
        self.copy_mode = True
        self.tmp_folder = None

    def meets_conditions(self):
        if not self.config.reader_options['name'].endswith('S3Reader') or not self.config.writer_options['name'].endswith('S3Writer'):
            raise RequisitesNotMet
        if not self.config.filter_before_options['name'].endswith('NoFilter'):
            raise RequisitesNotMet
        if not self.config.filter_after_options['name'].endswith('NoFilter'):
            raise RequisitesNotMet
        if not self.config.transform_options['name'].endswith('NoTransform'):
            raise RequisitesNotMet
        if not self.config.grouper_options['name'].endswith('NoGrouper'):
            raise RequisitesNotMet

    def _get_filebase(self, writer_options):
        dest_filebase = writer_options['filebase'].format(datetime.datetime.now())
        dest_filebase = datetime.datetime.now().strftime(dest_filebase)
        return dest_filebase

    def bypass(self):
        from copy import deepcopy
        reader_options = self.config.reader_options['options']
        writer_options = self.config.writer_options['options']
        dest_bucket = get_bucket(**writer_options)
        dest_filebase = self._get_filebase(writer_options)
        s3_persistence = S3BypassState(self.config)
        source_bucket = get_bucket(**reader_options)
        pending_keys = deepcopy(s3_persistence.pending_keys())
        try:
            for key in pending_keys:
                dest_key_name = '{}/{}'.format(dest_filebase, key.split('/')[-1])
                self._copy_key(dest_bucket, dest_key_name, source_bucket, key)
                s3_persistence.commit_copied_key(key)
                logging.log(logging.INFO,
                            'Copied key {} to dest: s3://{}/{}'.format(key, dest_bucket.name, dest_key_name))
        finally:
            if self.tmp_folder:
                shutil.rmtree(self.tmp_folder)

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
