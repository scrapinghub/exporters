import datetime
import os
import re
import uuid
from exporters.writers.base_writer import BaseWriter


class FilebaseBaseWriter(BaseWriter):
    def __init__(self, options):
        supported_options = getattr(self, 'supported_options')
        if 'filebase' not in supported_options:
            supported_options['filebase'] = {'type': basestring}
        self.supported_options = supported_options
        super(FilebaseBaseWriter, self).__init__(options)
        self.filebase = self.read_option('filebase')

    def get_file_suffix(self, path, prefix):
        return str(uuid.uuid4())

    def create_filebase_name(self, group_info, extension='gz'):
        normalized = [re.sub('\W', '_', s) for s in group_info]
        filebase = self.read_option('filebase').format(date=datetime.datetime.now(),
                                                       groups=normalized)
        filebase = datetime.datetime.now().strftime(filebase)
        filebase_path, prefix = os.path.split(filebase)
        filename = prefix + self.get_file_suffix(filebase_path, prefix) + '.' + extension
        return filebase_path, filename
