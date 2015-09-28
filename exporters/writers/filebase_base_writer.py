import datetime
import os
from exporters.writers.base_writer import BaseWriter


DEFAULT_FILENAME = 'export'


class FilebaseBaseWriter(BaseWriter):
    def __init__(self, options):
        supported_options = getattr(self, 'supported_options')
        supported_options['filebase'] = {'type': basestring}
        self.supported_options = supported_options
        super(FilebaseBaseWriter, self).__init__(options)
        self.filebase = self.read_option('filebase').format(datetime.datetime.now())
        self.filebase_path, self.filenames_prefix = os.path.split(self.filebase)
        if not self.filenames_prefix:
            self.filenames_prefix = DEFAULT_FILENAME

    def write(self, dump_path, group_key):
        raise NotImplementedError
