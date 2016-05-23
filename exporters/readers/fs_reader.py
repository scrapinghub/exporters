import gzip
import json
import os
import re
from io import BufferedReader

from exporters.readers.base_reader import BaseReader
from exporters.records.base_record import BaseRecord
from exporters.exceptions import ConfigurationError


class FSReader(BaseReader):
    """
    Reads items from files located in filesystem and compressed with gzip with a common path.

        - batch_size (int)
            Number of items to be returned in each batch

        - input (str/dict or a list of str/dict)
            Specification of files to be read.

            Accepts either one "input_unit" or many of them in a
            list. "input_unit" is defined as follows:

            If a string, it indicates a filename, e.g. "/path/to/filename".

            If a dictionary, it indicates a directory to be read with the
            following elements:

            - "dir": path to directory, e.g. "/path/to/dir".

            - "dir_pointer": path to file containing path to directory,
              e.g. "/path/to/pointer/file" which contains "/path/to/dir".
              Cannot be used together with "dir".

              For example:

                We have a weekly export set with CRON. If we wanted to point to
                a new data path every week, we should keep updating the export
                configuration. With a pointer, we can set the reader to read
                from that file, which contains one line with a valid path to
                datasets, so only that pointer file should be updated.

            - "pattern": (optional) regular expression to filter filenames,
              e.g. "output.*\.jl\.gz$"

    """

    # List of options to set up the reader
    supported_options = {
        'batch_size': {'type': int, 'default': 10000},
        'input': {'type': (str, dict, list), 'default': {'dir': ''}}
    }

    def __init__(self, *args, **kwargs):
        super(FSReader, self).__init__(*args, **kwargs)
        self.batch_size = self.read_option('batch_size')
        self.input_specification = self.read_option('input')
        self.lines_reader = self.read_lines_from_files()

        self.files = self._get_input_files(self.input_specification)
        self.read_files = []
        self.current_file = None
        self.last_line = 0
        self.logger.info('FSReader has been initiated')

    @classmethod
    def _get_input_files(cls, input_specification):
        """Get list of input files according to input definition.

        Input definition can be:

        - str: specifying a filename

        - list of str: specifying list a of filenames

        - dict with "dir" and optional "pattern" parameters: specifying the
        toplevel directory under which input files will be sought and an optional
        filepath pattern

        """
        if isinstance(input_specification, (basestring, dict)):
            input_specification = [input_specification]
        elif not isinstance(input_specification, list):
            raise ConfigurationError("Input specification must be string, list or dict.")

        out = []
        for input_unit in input_specification:
            if isinstance(input_unit, basestring):
                out.append(input_unit)
            elif isinstance(input_unit, dict):
                missing = object()
                directory = input_unit.get('dir', missing)
                dir_pointer = input_unit.get('dir_pointer', missing)
                if directory is missing and dir_pointer is missing:
                    raise ConfigurationError(
                        'Input directory dict must contain'
                        ' "dir" or "dir_pointer" element (but not both)')
                if directory is not missing and dir_pointer is not missing:
                    raise ConfigurationError(
                        'Input directory dict must not contain'
                        ' both "dir" and "dir_pointer" elements')
                if dir_pointer is not missing:
                    directory = cls._get_pointer(dir_pointer)

                out.extend(cls._get_directory_files(
                    directory, input_unit.get('pattern')))
            else:
                raise ConfigurationError('Input must only contain strings or dicts')
        return out

    def read_lines_from_files(self):
        """
        Open and reads files from self.files variable, and yields the items extracted from them.

        """
        if not self.files:
            self.logger.warning('Files not found for reading')

        for fpath in sorted(self.files):
            with gzip.open(fpath) as f:
                with BufferedReader(f) as bf:
                    for line in bf:
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

    @classmethod
    def _get_pointer(cls, path_pointer):
        """
        Given a pointer path extracts the path to read the datasets from
        """
        with open(path_pointer) as f:
            return f.read().strip()

    @classmethod
    def _get_directory_files(cls, directory, pattern=None):
        if pattern is None:
            def filepath_matches(x):
                return True
        else:
            filepath_matches = re.compile(pattern).search

        return [
            filepath
            for dirpath, directories, filenames in os.walk(directory)
            for filepath in (os.path.join(dirpath, f) for f in filenames)
            if filepath_matches(filepath)
        ]

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
