import datetime
import os
import re
import uuid
from exporters.writers.base_writer import BaseWriter


class FilebaseBaseWriter(BaseWriter):
    """
    This writer is a base writer providing common methods to all file based writers

    - filebase
        Path to store the exported files

    """
    supported_options = {'filebase': {'type': basestring}}

    def __init__(self, options, *args, **kwargs):
        super(FilebaseBaseWriter, self).__init__(options, *args, **kwargs)
        self.filebase = self.read_option('filebase')

    def get_file_suffix(self, path, prefix):
        """
        This method is a fallback to provide valid random filenames
        """
        return str(uuid.uuid4())

    def create_filebase_name(self, group_info, extension='gz'):
        """
        Returns filebase and file valid name
        """
        normalized = [re.sub('\W', '_', s) for s in group_info]
        filebase = self.read_option('filebase')
        filebase = filebase.format(date=datetime.datetime.now(),
                                                           groups=normalized)
        filebase = datetime.datetime.now().strftime(filebase)
        filebase_path, prefix = os.path.split(filebase)
        filename = prefix + self.get_file_suffix(filebase_path, prefix) + '.' + extension
        return filebase_path, filename
