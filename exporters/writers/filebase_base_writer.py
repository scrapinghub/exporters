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
        self.filename = self._get_filename()
        if not self.filename:
            self.filename = DEFAULT_FILENAME
        else:
            self.options['filebase'] = os.path.sep.join(
                self.read_option('filebase').split(os.path.sep)[:-1])

    def _get_filename(self):
        return self.read_option('filebase').format(datetime.datetime.now()).split(
            os.path.sep)[-1]

    def write(self, dump_path, group_key):
        raise NotImplementedError
