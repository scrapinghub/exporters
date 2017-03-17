import os
import shutil
import tempfile
import uuid
import re
from six.moves import UserDict

from exporters.compression import get_compress_file

from .utils import get_filename


class GroupingInfo(UserDict):
    """Contains groups metadata for the grouping feature in writers,
    tracking which group keys being used plus some details for each group:

    * how many items were written
    * which are the buffer files used
    * how many items are in the current buffer
    """
    used_random_strings = set()

    def _get_random_string(self, length=7):
        while True:
            s = str(uuid.uuid4())[:length]
            if s not in self.used_random_strings:
                break
        self.used_random_strings.add(s)
        return s

    def _init_group_info_key(self, key):
        clean_filename_re = r"^[\w\.\s\d_-]+$"
        self[key] = {}
        self[key]['membership'] = key
        groups = tuple(
            g_info if re.match(
                clean_filename_re, g_info) else self._get_random_string() for g_info in key)
        self[key]['path_safe_keys'] = groups
        self[key]['total_items'] = 0
        self[key]['buffered_items'] = 0
        self[key]['group_file'] = []

    def ensure_group_info(self, key):
        if key not in self:
            self._init_group_info_key(key)

    def add_buffer_file_to_group(self, key, buffer_file):
        self[key]['group_file'].append(buffer_file)

    def add_to_group(self, key):
        self[key]['total_items'] += 1
        self[key]['buffered_items'] += 1

    def reset_key(self, key):
        self[key]['buffered_items'] = 0

    def is_first_file_item(self, key):
        return self.get(key, {}).get('buffered_items', 0) == 0


class BufferFile(object):

    def __init__(self, formatter, tmp_folder, compression_format,
                 file_name=None, hash_algorithm='md5'):
        self.formatter = formatter
        self.tmp_folder = tmp_folder
        self.file_extension = formatter.file_extension
        self.compression_format = compression_format
        self.path = self._get_new_path_name(file_name)
        self.file = self._create_file()
        header = self.formatter.format_header()
        if header:
            self.file.write(header)

    def _create_file(self):
        return get_compress_file(self.compression_format)(self.path)

    def _get_new_path_name(self, file_name):
        if not file_name:
            file_name = get_filename(uuid.uuid4(), self.file_extension, self.compression_format)
        return os.path.join(self.tmp_folder, file_name)

    def add_item_to_file(self, item):
        content = self.formatter.format(item)
        self.file.write(content)

    def add_item_separator_to_file(self):
        content = self.formatter.item_separator
        self.file.write(content)

    def end_file(self):
        footer = self.formatter.format_footer()
        if footer:
            self.file.write(footer)
        self.file.close()


class GroupingBufferFilesTracker(object):
    """Class responsible for tracking buffer files
    used for grouping feature in writers components.

    Group buffer files are kept inside a temporary folder
    that is cleaned up when calling close().
    """

    def __init__(self, formatter, compression_format, **kwargs):
        self.grouping_info = self._create_grouping_info()
        self.file_extension = formatter.file_extension
        self.formatter = formatter
        self.tmp_folder = tempfile.mkdtemp()
        self.compression_format = compression_format

    def add_item_to_file(self, item, key):
        buffer_file = self.get_current_buffer_file_for_group(key)
        buffer_file.add_item_to_file(item)
        self.grouping_info.add_to_group(key)

    def add_item_separator_to_file(self, key):
        buffer_file = self.get_current_buffer_file_for_group(key)
        buffer_file.add_item_separator_to_file()

    def end_group_file(self, key):
        buffer_file = self.get_current_buffer_file_for_group(key)
        buffer_file.end_file()

    def close(self):
        shutil.rmtree(self.tmp_folder, ignore_errors=True)

    def create_new_group_file(self, key):
        new_buffer_file = self._create_buffer_file()
        self.grouping_info.add_buffer_file_to_group(key, new_buffer_file)
        self.grouping_info.reset_key(key)
        return new_buffer_file

    def get_current_buffer_file_for_group(self, key):
        if self.grouping_info[key]['group_file']:
            buffer_file = self.grouping_info[key]['group_file'][-1]
        else:
            buffer_file = self.create_new_group_file(key)
        return buffer_file

    def _create_grouping_info(self):
        return GroupingInfo()

    def _create_buffer_file(self, file_name=None):
        return BufferFile(self.formatter, self.tmp_folder,
                          self.compression_format, file_name=file_name)
