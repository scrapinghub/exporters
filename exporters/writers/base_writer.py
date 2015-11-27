from exporters.buffer_file_manager import BufferFileManager
from exporters.logger.base_logger import WriterLogger
from exporters.pipeline.base_pipeline_item import BasePipelineItem


class ItemsLimitReached(Exception):
    """
    This exception is thrown when the desired items number has been reached
    """


TEMP_FILES_NAME = 'temp'

ITEMS_PER_BUFFER_WRITE = 500000
SIZE_PER_BUFFER_WRITE = 0


class BaseWriter(BasePipelineItem):
    """
    This module receives a batch and writes it where needed. It adds an optionsl items_limit parameter to allow
     to limit the number of exported items. If set to 0, there is no limit.
    """
    supported_options = {
        'items_per_buffer_write': {'type': int, 'default': ITEMS_PER_BUFFER_WRITE},
        'size_per_buffer_write': {'type': int, 'default': SIZE_PER_BUFFER_WRITE},
        'items_limit': {'type': int, 'default': 0},
    }
    supported_file_extensions = {
        'csv': 'csv',
        'json': 'jl',
    }

    def __init__(self, options):
        super(BaseWriter, self).__init__(options)
        self.finished = False
        self.check_options()
        items_per_buffer_write = self.read_option('items_per_buffer_write')
        size_per_buffer_write = self.read_option('size_per_buffer_write')
        self.items_limit = self.read_option('items_limit')
        self.logger = WriterLogger({'log_level': options.get('log_level'),
                                    'logger_name': options.get('logger_name')})
        self.buffer_file_manager = BufferFileManager(self, items_per_buffer_write, size_per_buffer_write, self.items_limit)
        self.items_count = 0

    def write(self, path, key):
        """
        Receive path to temp dump file and group key, and write it to the proper location.
        """
        raise NotImplementedError

    def write_batch(self, batch):
        """
        Receive the batch and write it.
        """
        for item in batch:
            if self.buffer_file_manager.file_extension is None:
                self.buffer_file_manager.file_extension = self.supported_file_extensions[item.format]
            if item.header:
                self.buffer_file_manager.header_line = item.formatted
            else:
                self.buffer_file_manager.to_buffer(item)
                self.increment_written_items()
                if self.items_limit and self.items_limit == self.items_count:
                    self.stats.update(self.buffer_file_manager.stats)
                    raise ItemsLimitReached(
                        'Finishing job after items_limit reached:'
                        ' {} items written.'.format(self.items_count))
        self.stats.update(self.buffer_file_manager.stats)

    def close_writer(self):
        """
        Called to clean all possible tmp files created during the process.
        """
        try:
            self.buffer_file_manager.flush_buffer()
        finally:
            self.buffer_file_manager.close()
        self._check_write_consistency()

    @property
    def grouping_info(self):
        return self.buffer_file_manager.grouping_info

    def _check_write_consistency(self):
        self.logger.warning('Not checking write consistency')

    def increment_written_items(self):
        self.items_count += 1
