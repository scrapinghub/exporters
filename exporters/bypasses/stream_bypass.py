import logging
from collections import namedtuple
from contextlib import closing

from exporters.bypasses.base import BaseBypass
from exporters.module_loader import ModuleLoader
from exporters.iterio import cohere_stream

Stream = namedtuple('Stream', 'filename size meta')


class StreamBypassState(object):
    def __init__(self, config, metadata):
        module_loader = ModuleLoader()
        self.state = module_loader.load_persistence(config.persistence_options, metadata)
        self.state_position = self.state.get_last_position()
        if not self.state_position:
            self.done = []
            self.skipped = []
            self.stats = {'bytes_copied': 0}
            self.state.commit_position(self._get_state())
        else:
            self.done = []
            self.skipped = self.state_position['done']
            self.stats = self.state_position.get('stats', {'bytes_copied': 0})

    def _get_state(self):
        return dict(done=self.done, skipped=self.skipped, stats=self.stats)

    def commit_copied(self, stream):
        self.increment_bytes(stream.size)
        self.done.append(stream)
        self.state.commit_position(self._get_state())

    def increment_bytes(self, cnt):
        self.stats['bytes_copied'] += cnt

    def delete(self):
        self.state.delete()


class StreamBypass(BaseBypass):
    """
    Generic Bypass that streams the contents of the files instead of running
    them through the export pipeline.

    It should be transparent to user. Conditions are:

        - Reader module supports get_read_streams
        - Writer module supports write_stream
        - No filter modules are set up.
        - No transform module is set up.
        - No grouper module is set up.
        - writer has no option items_limit set in configuration.
        - writer has default items_per_buffer_write and size_per_buffer_write per default.
    """

    def __init__(self, config, metadata):
        super(StreamBypass, self).__init__(config, metadata)
        self.bypass_state = None
        self.logger = logging.getLogger('bypass_logger')
        self.logger.setLevel(logging.INFO)

    @classmethod
    def meets_conditions(cls, config):
        if not config.filter_before_options['name'].endswith('NoFilter'):
            cls._log_skip_reason('custom filter configured')
            return False
        if not config.filter_after_options['name'].endswith('NoFilter'):
            cls._log_skip_reason('custom filter configured')
            return False
        if not config.transform_options['name'].endswith('NoTransform'):
            cls._log_skip_reason('custom transform configured')
            return False
        if not config.grouper_options['name'].endswith('NoGrouper'):
            cls._log_skip_reason('custom grouper configured')
            return False
        if config.writer_options.get('options', {}).get('items_limit'):
            cls._log_skip_reason('items limit configuration (items_limit)')
            return False
        if config.writer_options.get('options', {}).get('items_per_buffer_write'):
            cls._log_skip_reason('buffer limit configuration (items_per_buffer_write)')
            return False
        if config.writer_options.get('options', {}).get('size_per_buffer_write'):
            cls._log_skip_reason('buffer limit configuration (size_per_buffer_write)')
            return False
        module_loader = ModuleLoader()
        try:
            with closing(module_loader.load_class(config.reader_options['name'])) as reader:
                pass
            with closing(module_loader.load_class(config.writer_options['name'])) as writer:
                pass
        except:
            cls._log_skip_reason("Can't load reader and/or writer")
            return False
        if not callable(getattr(reader, 'get_read_streams', None)) or\
           not callable(getattr(reader, 'open_stream', None)):
            cls._log_skip_reason("Reader doesn't support get_read_streams()/open_stream()")
            return False
        if not hasattr(writer, 'write_stream'):
            cls._log_skip_reason("Writer doesn't support write_stream()")
            return False
        return True

    def execute(self):
        # We can't count items on streamed bypasses
        self.valid_total_count = False
        self.bypass_state = StreamBypassState(self.config, self.metadata)
        module_loader = ModuleLoader()
        reader = module_loader.load_reader(self.config.reader_options, self.metadata)
        writer = module_loader.load_writer(self.config.writer_options, self.metadata)
        with closing(reader), closing(writer):
            for stream in reader.get_read_streams():
                if stream not in self.bypass_state.skipped:
                    file_obj = cohere_stream(reader.open_stream(stream))
                    logging.log(logging.INFO, 'Starting to copy file {}'.format(stream.filename))
                    try:
                        writer.write_stream(stream, file_obj)
                    finally:
                        file_obj.close()
                    logging.log(logging.INFO, 'Finished copying file {}'.format(stream.filename))
                    self.bypass_state.commit_copied(stream)
                else:
                    logging.log(logging.INFO, 'Skip file {}'.format(stream.filename))

    def close(self):
        if self.bypass_state:
            self.bypass_state.delete()
