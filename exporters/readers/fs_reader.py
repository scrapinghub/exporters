import gzip
import json
import os
import re
from exporters.readers.base_reader import BaseReader
from exporters.records.base_record import BaseRecord
from exporters.exceptions import ConfigurationError


class FSReader(BaseReader):
    """
    Reads items from s3 files with a common path.

        - batch_size (int)
            Number of items to be returned in each batch

        - path (str)
            Files path to be read.

        - path_pointer (str)
            Path pointing to the last version of dataset.

        - pattern (str)
            File name pattern (REGEX).

    """

    # List of options to set up the reader
    supported_options = {
        'batch_size': {'type': int, 'default': 10000},
        'path': {'type': basestring, 'default': ''},
        'path_pointer': {'type': basestring, 'default': None},
        'pattern': {'type': basestring, 'default': None}
    }

    def __init__(self, options):
        super(FSReader, self).__init__(options)
        self.batch_size = self.read_option('batch_size')
        self.path = self.read_option('path')
        self.path_pointer = self.read_option('path_pointer')
        self.pattern = self.read_option('pattern')
        self.lines_reader = self.read_lines_from_files()

        if self.path and self.path_pointer:
            raise ConfigurationError("path and path_pointer options cannot be used together")

        if self.path_pointer:
            self.path = self._get_pointer(self.path_pointer)

        self.files = []
        all_files = self._get_all_files_from_tree()
        for file in all_files:
            if self.pattern:
                self._add_file_if_matches(file)
            else:
                self.files.append(file)
        self.read_files = []
        self.current_file = None
        self.last_line = 0
        self.logger.info('FSReader has been initiated')

    def read_lines_from_files(self):
        for fpath in self.files:
            with gzip.open(fpath) as f:
                for line in f:
                    self.last_line += 1
                    line = line.replace("\n", '')
                    item = BaseRecord(json.loads(line))
                    yield item

            self.read_files.append(fpath)
            self.files.remove(fpath)
            self.current_file = None
            if len(self.files) == 0:
                self.finished = True
            self.last_position['files'] = self.files
            self.last_position['read_files'] = self.read_files
            self.last_position['current_file'] = self.current_file
            self.last_position['last_line'] = self.last_line

    def _get_pointer(self, path_pointer):
        with open(path_pointer) as f:
            return f.read().strip()

    def _get_all_files_from_tree(self):
        all_files = []
        for root, directories, filenames in os.walk(self.path):
            for filename in filenames:
                all_files.append(os.path.join(root,filename))
        return all_files

    def _add_file_if_matches(self, file):
        if re.match(os.path.join(self.path, self.pattern), file):
            self.files.append(file)

    def get_next_batch(self):
        count = 0
        while count < self.batch_size:
            count += 1
            yield next(self.lines_reader)
        self.logger.debug('Done reading batch')

    def set_last_position(self, last_position):
        if last_position is None:
            self.last_position = {}
            self.last_position['files'] = self.files
            self.last_position['read_files'] = self.read_files
            self.last_position['current_file'] = None
            self.last_position['last_line'] = 0
        else:
            self.last_position = last_position
            self.files = self.last_position['files']
            self.read_files = self.last_position['read_files']
            if self.last_position['current_file']:
                self.current_file = self.last_position['current_file']
            else:
                self.current_file = self.files[0]
                self.last_line = 0
            self.last_line = self.last_position['last_line']
