import six
from exporters.export_formatter import DEFAULT_FORMATTER_CLASS
from exporters.compression import FILE_COMPRESSION
from exporters.exceptions import ConfigurationError
from exporters.logger.base_logger import WriterLogger
from exporters.pipeline.base_pipeline_item import BasePipelineItem
from exporters.write_buffer import WriteBuffer, GroupingBufferFilesTracker


class ItemsLimitReached(Exception):
    """
    This exception is thrown when the desired items number has been reached
    """


class InconsistentWriteState(Exception):
    """
    This exception is thrown when write state is inconsistent with expected final state
    """


ITEMS_PER_BUFFER_WRITE = 500000
# Setting a default limit of 4Gb per file
SIZE_PER_BUFFER_WRITE = 4000000000


class BaseWriter(BasePipelineItem):
    """
    This module receives a batch and writes it where needed.
    """
    supported_options = {
        'items_per_buffer_write': {'type': six.integer_types, 'default': ITEMS_PER_BUFFER_WRITE},
        'size_per_buffer_write': {'type': six.integer_types, 'default': SIZE_PER_BUFFER_WRITE},
        'items_limit': {'type': six.integer_types, 'default': 0},
        'check_consistency': {'type': bool, 'default': False},
        'compression': {'type': six.string_types, 'default': 'gz'}
    }

    hash_algorithm = None

    def __init__(self, options, metadata, *args, **kwargs):
        super(BaseWriter, self).__init__(options, metadata, *args, **kwargs)
        self.finished = False
        self.check_options()
        self.items_limit = self.read_option('items_limit')
        self.logger = WriterLogger({'log_level': options.get('log_level'),
                                    'logger_name': options.get('logger_name')})
        self.export_formatter = kwargs.get('export_formatter')
        if self.export_formatter is None:
            self.export_formatter = DEFAULT_FORMATTER_CLASS(options=dict(), metadata=metadata)
        items_per_buffer_write = self.read_option('items_per_buffer_write')
        size_per_buffer_write = self.read_option('size_per_buffer_write')
        self.compression_format = self._get_compression_format()
        self.write_buffer = WriteBuffer(items_per_buffer_write,
                                        size_per_buffer_write,
                                        self._items_group_files_handler(),
                                        self.compression_format, self.hash_algorithm)
        self.set_metadata('items_count', 0)

    def _get_compression_format(self):
        compression = self.read_option('compression')
        if compression not in FILE_COMPRESSION:
            raise ConfigurationError('The compression format can only be '
                                     'one of the following:  "{}"'
                                     ''.format(FILE_COMPRESSION.keys()))
        return compression

    def _items_group_files_handler(self):
        return GroupingBufferFilesTracker(self.export_formatter, self.compression_format)

    def write(self, path, key):
        """
        Receive path to buffer file and group key and write its contents
        to the configured destination.

        Should be implemented in derived classes.

        It's called when it's time to flush a buffer, usually by
        either write_batch() or flush() methods.
        """
        raise NotImplementedError

    def write_batch(self, batch):
        """
        Buffer a batch of items to be written and update internal counters.

        Calling this method doesn't guarantee that all items have been written.
        To ensure everything has been written you need to call flush().
        """
        for item in batch:
            self.write_buffer.buffer(item)
            key = self.write_buffer.get_key_from_item(item)
            if self.write_buffer.should_write_buffer(key):
                self._write_current_buffer_for_group_key(key)
            self.increment_written_items()
            self._check_items_limit()

    def _check_items_limit(self):
        """
        Raise ItemsLimitReached if the writer reached the configured items limit.
        """
        if self.items_limit and self.items_limit == self.get_metadata('items_count'):
            raise ItemsLimitReached('Finishing job after items_limit reached:'
                                    ' {} items written.'.format(self.get_metadata('items_count')))

    def _should_flush(self, key):
        return self.grouping_info[key].get('buffered_items', 0) > 0

    def flush(self):
        """
        Ensure all remaining buffers are written.
        """
        for key in self.grouping_info.keys():
            if self._should_flush(key):
                self._write_current_buffer_for_group_key(key)

    def close(self):
        """
        Close all buffers, cleaning all temporary files.
        """
        if self.write_buffer is not None:
            self.write_buffer.close()

    @property
    def grouping_info(self):
        if self.write_buffer is not None:
            return self.write_buffer.grouping_info
        else:
            return {}

    def _check_write_consistency(self):
        """
        Should be overidden by derived classes to add support for
        consistency checks (enabled by option check_consistency).

        The default implementation just logs a warning and
        doesn't do any checks.
        """
        self.logger.warning('Not checking write consistency')

    def increment_written_items(self):
        self.set_metadata('items_count', self.get_metadata('items_count') + 1)

    def _write_current_buffer_for_group_key(self, key):
        """
        Find the buffer for a given group key, prepare it to be written
        and writes it calling write() method.
        """
        write_info = self.write_buffer.pack_buffer(key)
        self.write(write_info.get('file_path'),
                   self.write_buffer.grouping_info[key]['membership'])
        self.write_buffer.clean_tmp_files(write_info)
        self.write_buffer.add_new_buffer_for_group(key)

    def finish_writing(self):
        """
        This method is hook for operations to be done after everything
        has been written (e.g. consistency checks, write a checkpoint, etc).

        The default implementation calls self._check_write_consistency
        if option check_consistency is True.
        """
        if self.read_option('check_consistency'):
            self._check_write_consistency()

    def set_metadata(self, key, value, module='writer'):
        super(BaseWriter, self).set_metadata(key, value, module)

    def update_metadata(self, data, module='writer'):
        super(BaseWriter, self).update_metadata(data, module)

    def get_metadata(self, key, module='writer'):
        return super(BaseWriter, self).get_metadata(key, module)

    def get_all_metadata(self, module='writer'):
        return super(BaseWriter, self).get_all_metadata(module)
