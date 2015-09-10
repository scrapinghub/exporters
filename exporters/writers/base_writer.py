import gzip
import os
import shutil
import uuid
from exporters.logger.base_logger import WriterLogger
from exporters.pipeline.base_pipeline_item import BasePipelineItem
import tempfile

TEMP_FILES_NAME = 'temp'

ITEMS_PER_SAVE = 10000


class ItemsLimitReached(Exception):
    """
    This exception is thrown when the desired items number has been reached
    """


class NoGroup(object):
    def __call__(self, batch): return {'': batch}

    def __repr__(self): return "NoGroup(  )"

    def __nonzero__(self): return 0

    def __getattr__(self, name): return self


class BaseWriter(BasePipelineItem):
    """
    This module receives a batch and writes it where needed. It adds an optionsl items_limit parameter to allow
     to limit the number of exported items. If set to 0, there is no limit.
    """

    def __init__(self, options, settings):
        super(BaseWriter, self).__init__(options, settings)
        self.settings = settings
        self.finished = False
        self.requirements = getattr(self, 'requirements', {})
        # If it's not there, we add it as a not mandatory requirement to publish it via config api
        if 'items_limit' not in self.requirements:
            self.requirements['items_limit'] = {'type': int, 'required': False, 'default': 0}
        self.options['options'] = self.options.get('options', {})
        self.tmp_folder = tempfile.mkdtemp()
        self.check_options()
        self.items_per_save = self.options.get('items_per_save', ITEMS_PER_SAVE)
        self.items_limit = self.options.get('items_limit', 0)
        self.logger = WriterLogger(self.settings)
        self.items_count = 0
        self.grouping_info = {}


    def write(self, path, key):
        """
        It receives where the tmp dump file is stored and group information, and it must write it wherever needed.
        """
        raise NotImplementedError

    def write_batch(self, batch):
        """
        It receives the batch and writes it.
        """
        for item in batch:
            self._write_item(item)

    def _is_save_needed(self, key):
        return self.grouping_info[key].get('predump_items', 0) >= self.items_per_save

    def _write_item(self, item):
        """
        It receives an item and writes it.
        """
        key = tuple(item.group_membership)
        if key not in self.grouping_info:
            self.grouping_info[key] = {}
            self.grouping_info[key]['membership'] = item.group_membership
            self.grouping_info[key]['total_items'] = 0
            self.grouping_info[key]['predump_items'] = 0
            self.grouping_info[key]['group_file'] = []

        self._write_and_save(item, key)
        self.items_count += 1
        if self.items_limit and self.items_limit == self.items_count:
            raise ItemsLimitReached('Finishing job after items_limit reached: {} items written.'.format(self.items_count))

    def _get_group_path(self, key):
        if self.grouping_info[key]['group_file']:
            path = self.grouping_info[key]['group_file'][-1]
        else:
            path = os.path.join(self.tmp_folder, str(uuid.uuid4()))
            self.grouping_info[key]['group_file'].append(path)
        return path

    def _write_and_save(self, item, key):
        path = self._get_group_path(key)
        with open(path, 'a') as f:
            f.write(item.formatted+'\n')
        self.grouping_info[key]['total_items'] += 1
        self.grouping_info[key]['predump_items'] += 1
        if self._is_save_needed(key):
            self.logger.debug('Save is needed.')
            self._save(key)
            self._reset_key(key)

    def _save(self, key):
        path = self._get_group_path(key)
        with gzip.open(path+'.gz', 'wb') as predump_file:
            with open(path) as fl:
                predump_file.write(fl.read())
        self.write(path+'.gz', self.grouping_info[key]['membership'])
        self.grouping_info[key]['group_file'].append(os.path.join(self.tmp_folder, str(uuid.uuid4())))

    def _reset_key(self, key):
        self.grouping_info[key]['predump_items'] = 0

    def close_writer(self):
        """
        Called to clean all possible tmp files created during the process.
        """
        for key in self.grouping_info.keys():
            self._save(key)
        shutil.rmtree(self.tmp_folder, ignore_errors=True)
