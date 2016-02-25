import hashlib
import json
import os
from exporters.write_buffer import WriteBuffer
from exporters.logger.base_logger import WriterLogger
from exporters.pipeline.base_pipeline_item import BasePipelineItem


class ItemsLimitReached(Exception):
    """
    This exception is thrown when the desired items number has been reached
    """


ITEMS_PER_BUFFER_WRITE = 500000
# Setting a default limit of 4Gb per file
SIZE_PER_BUFFER_WRITE = 4000000000


class BaseWriter(BasePipelineItem):
    """
    This module receives a batch and writes it where needed.
    """
    supported_options = {
        'items_per_buffer_write': {'type': int, 'default': ITEMS_PER_BUFFER_WRITE},
        'size_per_buffer_write': {'type': int, 'default': SIZE_PER_BUFFER_WRITE},
        'items_limit': {'type': int, 'default': 0},
        'file_info_path': {'type': basestring, 'default': None}
    }

    def __init__(self, options, *args, **kwargs):
        super(BaseWriter, self).__init__(options, *args, **kwargs)
        self.finished = False
        self.check_options()
        self.items_limit = self.read_option('items_limit')
        self.logger = WriterLogger({'log_level': options.get('log_level'),
                                    'logger_name': options.get('logger_name')})
        self.export_formatter = kwargs.get('export_formatter')
        items_per_buffer_write = self.read_option('items_per_buffer_write')
        size_per_buffer_write = self.read_option('size_per_buffer_write')
        self.write_buffer = WriteBuffer(items_per_buffer_write, size_per_buffer_write, self.export_formatter)
        self.writer_metadata = {
            'items_count': 0
        }
        self.file_info_path = self.read_option('file_info_path')
        if self.file_info_path:
            with open(self.file_info_path, 'w'):
                pass

    def write(self, path, key):
        """
        Receive path to temp dump file and group key, and write it to the proper location.
        """
        raise NotImplementedError

    def write_batch(self, batch):
        """
        Receives the batch and writes it. This method is usually called from a manager.
        """
        for item in batch:
            self.write_buffer.buffer(item)
            key = self.write_buffer.get_key_from_item(item)
            if self.write_buffer.should_write_buffer(key):
                self._write(key)
            self.increment_written_items()
            self._check_items_limit()
        self.stats.update(self.write_buffer.stats)

    def _check_items_limit(self):
        """
        Check if a writer has reached the items limit. If so, it raises an ItemsLimitReached
        exception
        """
        if self.items_limit and self.items_limit == self.writer_metadata['items_count']:
            self.stats.update(self.write_buffer.stats)
            raise ItemsLimitReached('Finishing job after items_limit reached:'
                                    ' {} items written.'.format(self.writer_metadata['items_count']))

    def flush(self):
        """
        This method trigers a key write.
        """
        for key in self.grouping_info.keys():
            self._write(key)

    def close(self):
        """
        Called to clean all possible tmp files created during the process.
        """
        if self.write_buffer is not None:
            self.write_buffer.close()
        self._check_write_consistency()

    @property
    def grouping_info(self):
        if self.write_buffer is not None:
            return self.write_buffer.grouping_info
        else:
            return {}

    def _check_write_consistency(self):
        """
        This should be overwridden if a write consistency check is needed
        """
        self.logger.warning('Not checking write consistency')

    def increment_written_items(self):
        self.writer_metadata['items_count'] += 1

    def _append_md5_info(self, write_info):
        file_name = self.writer_metadata['written_files'][-1]
        with open(file_name, 'r') as f:
            md5 = hashlib.md5(f.read()).hexdigest()
        with open(self.file_info_path, 'a') as f:
            f.write('{} {}'.format(md5, file_name)+'\n')

    def _write(self, key):
        write_info = self.write_buffer.pack_buffer(key)
        self.write(write_info.get('compressed_path'), self.write_buffer.grouping_info[key]['membership'])
        if self.file_info_path and self.writer_metadata.get('written_files'):
            self._append_md5_info(write_info)
        self.write_buffer.clean_tmp_files(key, write_info.get('compressed_path'))
