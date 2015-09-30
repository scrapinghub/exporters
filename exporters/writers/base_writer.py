import gzip
import os
import shutil
import uuid
from exporters.logger.base_logger import WriterLogger
from exporters.pipeline.base_pipeline_item import BasePipelineItem
import tempfile

TEMP_FILES_NAME = 'temp'

ITEMS_PER_BUFFER_WRITE = 500000
SIZE_PER_BUFFER_WRITE = 0


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
    base_supported_options = {
        'items_per_buffer_write': {'type': int, 'default': ITEMS_PER_BUFFER_WRITE},
        'size_per_buffer_write': {'type': int, 'default': SIZE_PER_BUFFER_WRITE},
        'items_limit': {'type': int, 'default': 0},
    }

    def __init__(self, options):
        super(BaseWriter, self).__init__(options)
        self.finished = False
        self.tmp_folder = tempfile.mkdtemp()
        self.check_options()
        self.items_per_buffer_write = self.read_option('items_per_buffer_write')
        self.size_per_buffer_write = self.read_option('size_per_buffer_write')
        self.items_limit = self.read_option('items_limit')
        self.logger = WriterLogger({'log_level': options.get('log_level'), 'logger_name': options.get('logger_name')})
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
            self._send_item_to_buffer(item)

    def _should_write_buffer(self, key):
        if self.size_per_buffer_write and os.path.getsize(
                self.grouping_info[key]['group_file'][-1]) >= self.size_per_buffer_write:
            return True
        return self.grouping_info[key].get('buffered_items', 0) >= self.items_per_buffer_write

    def _send_item_to_buffer(self, item):
        """
        It receives an item and writes it.
        """
        key = tuple(item.group_membership)
        if key not in self.grouping_info:
            self.grouping_info[key] = {}
            self.grouping_info[key]['membership'] = item.group_membership
            self.grouping_info[key]['total_items'] = 0
            self.grouping_info[key]['buffered_items'] = 0
            self.grouping_info[key]['group_file'] = []

        self._add_to_buffer(item, key)
        if self._should_write_buffer(key):
            self.logger.debug('Buffer write is needed.')
            self._write_buffer(key)
        self.items_count += 1
        if self.items_limit and self.items_limit == self.items_count:
            raise ItemsLimitReached(
                'Finishing job after items_limit reached: {} items written.'.format(self.items_count))

    def _get_group_path(self, key):
        if self.grouping_info[key]['group_file']:
            path = self.grouping_info[key]['group_file'][-1]
        else:
            path = self._get_new_path_name()
            self.grouping_info[key]['group_file'].append(path)
        return path

    def _add_to_buffer(self, item, key):
        path = self._get_group_path(key)
        with open(path, 'a') as f:
            f.write(item.formatted + '\n')
        self.grouping_info[key]['total_items'] += 1
        self.grouping_info[key]['buffered_items'] += 1

    def _compress_file(self, path):
        compressed_path = path + '.gz'
        with gzip.open(compressed_path, 'wb') as dump_file, open(path) as fl:
            shutil.copyfileobj(fl, dump_file)
        return compressed_path

    def _create_buffer_path_for_key(self, key):
        new_buffer_path = self._get_new_path_name()
        self.grouping_info[key]['group_file'].append(new_buffer_path)
        f = open(new_buffer_path, 'w')
        f.close()

    def _get_new_path_name(self):
        return os.path.join(self.tmp_folder, str(uuid.uuid4())+'.jl')

    def _write_buffer(self, key):
        path = self._get_group_path(key)
        compressed_path = self._compress_file(path)
        self.write(compressed_path, self.grouping_info[key]['membership'])
        self._create_buffer_path_for_key(key)
        self._reset_key(key)

    def _reset_key(self, key):
        self.grouping_info[key]['buffered_items'] = 0

    def close_writer(self):
        """
        Called to clean all possible tmp files created during the process.
        """
        for key in self.grouping_info.keys():
            self._write_buffer(key)
        shutil.rmtree(self.tmp_folder, ignore_errors=True)
