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

    @property
    def filebase_path(self):
        if self.filebase.split(os.path.sep)[-1]:
            return os.path.sep.join(self.filebase.split(os.path.sep)[:-1])
        else:
            return self.filebase

    @property
    def prefix(self):
        return self.read_option('filebase').format(datetime.datetime.now()).split(
            os.path.sep)[-1] or DEFAULT_FILENAME

    def write(self, dump_path, group_key):
        raise NotImplementedError
