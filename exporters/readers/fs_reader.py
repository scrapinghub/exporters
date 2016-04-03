import gzip
import json
import os
import re
from exporters.readers.base_reader import BaseReader
from exporters.records.base_record import BaseRecord
from exporters.exceptions import ConfigurationError


class FSReader(BaseReader):
    """
    Reads items from files located in filesystem and compressed with gzip with a common path.

        - batch_size (int)
            Number of items to be returned in each batch

        - path (str)
            Files path to be read. This reader will read recusively inside this path.

        - path_pointer (str)
            Path pointing to the last version of dataset. This adds support for regular exports.
            For example:
                We have a weekly export set with CRON. If we wanted to point to a new data
                path every week, we should keep updating the export configuration. With a pointer,
                we can set the reader to read from that file, which contains one line with
                a valid path to datasets, so only that pointer file should be updated.

        - pattern (str)
            File name pattern (REGEX). All files that don't match this regex string will be
            discarded by the reader.
    """

    # List of options to set up the reader
    supported_options = {
        'batch_size': {'type': int, 'default': 10000},
        'path': {'type': basestring, 'default': ''},
        'path_pointer': {'type': basestring, 'default': None},
        'pattern': {'type': basestring, 'default': None}
    }

    def __init__(self, *args, **kwargs):
        super(FSReader, self).__init__(*args, **kwargs)
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
        """
        Open and reads files from self.files variable, and yields the items extracted from them.

        """
        if not self.files:
            self.logger.warning('Files not found for reading')

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
            self.last_position['files'] = self.files
            self.last_position['read_files'] = self.read_files
            self.last_position['current_file'] = self.current_file
            self.last_position['last_line'] = self.last_line

        self.finished = True

    def _get_pointer(self, path_pointer):
        """
        Given a pointer path extracts the path to read the datasets from
        """
        with open(path_pointer) as f:
            return f.read().strip()

    def _get_all_files_from_tree(self):
        """
        Returns a list of files under a given path
        """
        all_files = []
        for root, directories, filenames in os.walk(self.path):
            for filename in filenames:
                all_files.append(os.path.join(root, filename))
        return all_files

    def _add_file_if_matches(self, file):
        """
        Checks if a filename matches the provided regex pattern
        """
        if re.match(os.path.join(self.path, self.pattern), file):
            self.files.append(file)

    def get_next_batch(self):
        """
        This method is called from the manager. It must return a list or a generator
        of BaseRecord objects.
        When it has nothing else to read, it must set class variable "finished" to True.
        """
        count = 0
        while count < self.batch_size:
            count += 1
            yield next(self.lines_reader)
        self.logger.debug('Done reading batch')

    def set_last_position(self, last_position):
        """
        Called from the manager, it is in charge of updating the last position of data commited
        by the writer, in order to have resume support
        """
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
