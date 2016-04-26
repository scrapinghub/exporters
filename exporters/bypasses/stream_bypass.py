import logging

from exporters.export_managers.base_bypass import RequisitesNotMet, BaseBypass
from exporters.module_loader import ModuleLoader


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

    def commit_copied(self, name, size):
        self.increment_bytes(size)
        self.done.append(name)
        self.state.commit_position(self._get_state())

    def increment_bytes(self, cnt):
        self.stats['bytes_copied'] += cnt

    def delete(self):
        self.state.delete()


def fix_fileobj(fileobj):
    """
    Adds a tell() method if the file doesn't have one already.
    This is accomplished by monkey-patching read()
    """
    if not hasattr(fileobj, 'tell') and not hasattr(fileobj, 'seek'):
        old_read = fileobj.read

        def new_read(num):
            buf = old_read(num)
            new_read.pos = new_read.pos + len(buf)
            return buf
        new_read.pos = 0

        fileobj.read = new_read
        fileobj.tell = lambda: new_read.pos


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
            raise RequisitesNotMet('custom filter configured')
        if not config.filter_after_options['name'].endswith('NoFilter'):
            raise RequisitesNotMet('custom filter configured')
        if not config.transform_options['name'].endswith('NoTransform'):
            raise RequisitesNotMet('custom transform configured')
        if not config.grouper_options['name'].endswith('NoGrouper'):
            raise RequisitesNotMet('custom grouper configured')
        if config.writer_options['options'].get('items_limit'):
            raise RequisitesNotMet('items limit configuration (items_limit)')
        if config.writer_options['options'].get('items_per_buffer_write'):
            raise RequisitesNotMet('buffer limit configuration (items_per_buffer_write)')
        if config.writer_options['options'].get('size_per_buffer_write'):
            raise RequisitesNotMet('buffer limit configuration (size_per_buffer_write)')

        module_loader = ModuleLoader()
        try:
            reader = module_loader.load_class(config.reader_options['name'])
            writer = module_loader.load_class(config.writer_options['name'])
        except:
            raise RequisitesNotMet("Can't load reader and/or writer")

        if not hasattr(reader, 'get_read_streams'):
            raise RequisitesNotMet("Reader doesn't support get_read_streams()")

        if not hasattr(writer, 'write_stream'):
            raise RequisitesNotMet("Writer doesn't support write_stream()")

    def execute(self):
        self.bypass_state = StreamBypassState(self.config, self.metadata)
        module_loader = ModuleLoader()
        reader = module_loader.load_reader(self.config.reader_options, self.metadata)
        writer = module_loader.load_writer(self.config.writer_options, self.metadata)

        for origin, name, size in reader.get_read_streams():
            if name not in self.bypass_state.skipped:
                fix_fileobj(origin)
                logging.log(logging.INFO, 'Starting to copy file {}'.format(name))
                try:
                    writer.write_fileobj(origin, name, size)
                finally:
                    if hasattr(origin, 'close'):
                        origin.close()
                logging.log(logging.INFO, 'Finished copying file {}'.format(name))
                self.bypass_state.commit_copied(name, size)
            else:
                logging.log(logging.INFO, 'Skip file {}'.format(name))

    def close(self):
        if self.bypass_state:
            self.bypass_state.delete()
