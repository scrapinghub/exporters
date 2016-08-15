import os
import re

from exporters.readers.base_stream_reader import StreamBasedReader
from exporters.exceptions import ConfigurationError
from exporters.bypasses.stream_bypass import Stream


class FSReader(StreamBasedReader):
    """
    Reads items from files located in filesystem and compressed with gzip with a common path.

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
        'input': {'type': (str, dict, list), 'default': {'dir': ''}}
    }

    def __init__(self, *args, **kwargs):
        super(FSReader, self).__init__(*args, **kwargs)
        self.input_specification = self.read_option('input')

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
                    directory=directory,
                    pattern=input_unit.get('pattern'),
                    include_dot_files=input_unit.get('include_dot_files', False)))
            else:
                raise ConfigurationError('Input must only contain strings or dicts')
        return out

    @classmethod
    def _get_pointer(cls, path_pointer):
        """
        Given a pointer path extracts the path to read the datasets from
        """
        with open(path_pointer) as f:
            return f.read().strip()

    @classmethod
    def _get_directory_files(cls, directory, pattern=None,
                             include_dot_files=False):
        match_funcs = []
        if pattern is not None:
            match_funcs.append(re.compile(pattern).search)

        if not include_dot_files:
            def is_non_dot_file(filepath):
                return not os.path.basename(filepath).startswith('.')

            match_funcs.append(is_non_dot_file)

        return [
            filepath
            for dirpath, directories, filenames in os.walk(directory)
            for filepath in (os.path.join(dirpath, f) for f in filenames)
            if all(mf(filepath) for mf in match_funcs)
        ]

    def open_stream(self, stream):
        return open(stream.filename, 'rb')

    def get_read_streams(self):
        for fpath in sorted(self.files):
            size = os.path.getsize(fpath)
            yield Stream(fpath, size, None)
