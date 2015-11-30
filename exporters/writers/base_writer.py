from exporters.write_buffer import WriteBuffer
from exporters.logger.base_logger import WriterLogger
from exporters.pipeline.base_pipeline_item import BasePipelineItem


class ItemsLimitReached(Exception):
    """
    This exception is thrown when the desired items number has been reached
    """


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
        self.write_buffer = WriteBuffer(items_per_buffer_write, size_per_buffer_write)
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
            if self.write_buffer.file_extension is None:
                self.write_buffer.file_extension = self.supported_file_extensions[item.format]
            if item.header:
                self.write_buffer.header_line = item.formatted
            else:
                self.write_buffer.buffer(item)
                key = self.write_buffer.get_key_from_item(item)
                if self.write_buffer.should_write_buffer(key):
                    self._write(key)

                self.increment_written_items()
                self.check_items_limit()
        self.stats.update(self.write_buffer.stats)

    def check_items_limit(self):
        if self.items_limit and self.items_limit == self.items_count:
            self.stats.update(self.write_buffer.stats)
            raise ItemsLimitReached('Finishing job after items_limit reached:'
                                    ' {} items written.'.format(self.items_count))

    def close(self):
        """
        Called to clean all possible tmp files created during the process.
        """
        try:
            for key in self.grouping_info.keys():
                self._write(key)
        finally:
            self.write_buffer.close()
        self._check_write_consistency()

    @property
    def grouping_info(self):
        return self.write_buffer.grouping_info

    def _check_write_consistency(self):
        self.logger.warning('Not checking write consistency')

    def increment_written_items(self):
        self.items_count += 1

    def _write(self, key):
        compressed_path = self.write_buffer.compress_key_path(key)
        self.write(compressed_path, self.write_buffer.grouping_info[key]['membership'])
        self.write_buffer.finish_buffer_write(key, compressed_path)
