import os
import shutil
import tempfile
import uuid
import re
import hashlib
from six.moves import UserDict

from exporters.compression import get_compress_file
from exporters.utils import remove_if_exists


def get_filename(name_without_ext, file_extension, compression_format):
    if compression_format != 'none':
        return '{}.{}.{}'.format(name_without_ext, file_extension, compression_format)
    else:
        return '{}.{}'.format(name_without_ext, file_extension)


def hash_for_file(path, algorithm, block_size=256*128):
    hash = hashlib.new(algorithm)
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(block_size), b''):
            hash.update(chunk)
    return hash.hexdigest()


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

    def __init__(self, formatter, compression_format):
        self.grouping_info = GroupingInfo()
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
        new_buffer_file = BufferFile(self.formatter, self.tmp_folder, self.compression_format)
        self.grouping_info.add_buffer_file_to_group(key, new_buffer_file)
        self.grouping_info.reset_key(key)
        return new_buffer_file

    def get_current_buffer_file_for_group(self, key):
        if self.grouping_info[key]['group_file']:
            buffer_file = self.grouping_info[key]['group_file'][-1]
        else:
            buffer_file = self.create_new_group_file(key)
        return buffer_file


class WriteBuffer(object):

    def __init__(self, items_per_buffer_write, size_per_buffer_write,
                 items_group_files_handler, compression_format='gz',
                 hash_algorithm=None):
        self.files = []
        self.items_per_buffer_write = items_per_buffer_write
        self.size_per_buffer_write = size_per_buffer_write
        self.hash_algorithm = hash_algorithm
        self.items_group_files = items_group_files_handler
        self.compression_format = compression_format
        self.metadata = {}
        self.is_new_buffer = True

    def buffer(self, item):
        """
        Receive an item and write it.
        """
        key = self.get_key_from_item(item)
        if not self.grouping_info.is_first_file_item(key):
            self.items_group_files.add_item_separator_to_file(key)
        self.grouping_info.ensure_group_info(key)
        self.items_group_files.add_item_to_file(item, key)

    def finish_buffer_write(self, key):
        self.items_group_files.end_group_file(key)

    def pack_buffer(self, key):
        """Prepare current buffer file for group of given key to be written
        (by gathering statistics).
        """
        self.finish_buffer_write(key)
        file_path = self.items_group_files.get_current_buffer_file_for_group(key).path
        file_hash = None
        if self.hash_algorithm:
            file_hash = hash_for_file(file_path, self.hash_algorithm)

        file_size = os.path.getsize(file_path)
        write_info = {
            'number_of_records': self.grouping_info[key]['buffered_items'],
            'file_path': file_path,
            'size': file_size,
            'file_hash': file_hash,
        }
        self.metadata[file_path] = write_info
        return write_info

    def add_new_buffer_for_group(self, key):
        self.items_group_files.create_new_group_file(key)

    def clean_tmp_files(self, write_info):
        remove_if_exists(write_info.get('path'))
        remove_if_exists(write_info.get('file_path'))

    def should_write_buffer(self, key):
        if self.size_per_buffer_write and os.path.getsize(
                self.grouping_info[key]['group_file'][-1].path) >= self.size_per_buffer_write:
            return True
        buffered_items = self.grouping_info[key].get('buffered_items', 0)
        return buffered_items >= self.items_per_buffer_write

    def close(self):
        self.items_group_files.close()

    def get_key_from_item(self, item):
        return tuple(item.group_membership)

    @property
    def grouping_info(self):
        return self.items_group_files.grouping_info

    def get_metadata(self, buffer_path, meta_key):
        return self.metadata.get(buffer_path, {}).get(meta_key)

    def set_metadata_for_file(self, file_name, **kwargs):
        if file_name not in self.metadata:
            self.metadata[file_name] = {}
        self.metadata[file_name].update(**kwargs)
