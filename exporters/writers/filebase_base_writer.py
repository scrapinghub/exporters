import datetime
import os
import re
from exporters.writers.base_writer import BaseWriter


class FilebaseBaseWriter(BaseWriter):
    def __init__(self, options):
        supported_options = getattr(self, 'supported_options')
        if 'filebase' not in supported_options:
            supported_options['filebase'] = {'type': basestring}
        self.supported_options = supported_options
        super(FilebaseBaseWriter, self).__init__(options)
        self.filebase = self.read_option('filebase')

    def _get_file_number(self, path, filename, number_of_digits=4):
        raise NotImplementedError

    def create_filebase_name(self, group_info, extension='gz'):
        normalized = [re.sub('\W', '_', s) for s in group_info]
        filebase = self.read_option('filebase').format(datetime.datetime.now(),
                                                       group=normalized)
        filebase_path, filename = os.path.split(filebase)
        filename += self._get_file_number(filebase_path, filename) + '.' + extension
        return filebase_path, filename
