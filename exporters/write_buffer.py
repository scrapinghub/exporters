import os
import shutil
import tempfile
import uuid
import re
import hashlib
from six.moves import UserDict

from exporters.compression import get_compress_func
from exporters.utils import remove_if_exists


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

    def add_path_to_group(self, key, path):
        self[key]['group_file'].append(path)

    def add_to_group(self, key):
        self[key]['total_items'] += 1
        self[key]['buffered_items'] += 1

    def reset_key(self, key):
        self[key]['buffered_items'] = 0


class HashFile(object):
    """
    file-like object that wraps around a file-like object and calculates
    the writed content hash.
    """
    def __init__(self, fl, algorithm):
        self._file = fl
        self.hash = hashlib.new(algorithm)

    def write(self, data):
        self._file.write(data)
        self.hash.update(data)

    def __getattr__(self, attr):
        return getattr(self._file, attr)


class ItemsGroupFilesHandler(object):
    """Class responsible for tracking buffer files
    used for grouping feature in writers components.

    Group buffer files are kept inside a temporary folder
    that is cleaned up when calling close().

    Problems:

    This class is currently also responsible for formatting
    items and writing them to the buffer files, which is not
    cool because it hurts the Single Responsibility Principle.

    It doesn't have a well-defined responsibility, which is
    its method names aren't immediately understandable

    Also, it's opening and closing the files every time it
    needs to append something, which is kinda unsafe (and bad
    for performance too), we could just keep the file opened and
    keep writing to it until we're done and then we'd close it.

    To aggravate the problem, there is now a derived class
    in FilebaseBaseWriter that must be considered when refactoring this.
    """

    def __init__(self, formatter):
        self.grouping_info = GroupingInfo()
        self.file_extension = formatter.file_extension
        self.formatter = formatter
        self.tmp_folder = tempfile.mkdtemp()

    def _add_to_file(self, content, key):
        path = self.get_current_buffer_path_for_group(key)
        with open(path, 'a') as f:
            f.write(content + '\n')
        self.grouping_info.add_to_group(key)

    def add_item_to_file(self, item, key):
        content = self.formatter.format(item)
        self._add_to_file(content, key)

    def end_group_file(self, key):
        path = self.get_current_buffer_path_for_group(key)
        footer = self.formatter.format_footer()
        if footer:
            with open(path, 'a') as f:
                f.write(footer)
        return path

    def close(self):
        shutil.rmtree(self.tmp_folder, ignore_errors=True)

    def create_new_group_file(self, key):
        path = self.create_new_group_path_for_key(key)
        self.grouping_info.reset_key(key)
        header = self.formatter.format_header()
        if header:
            with open(path, 'w') as f:
                f.write(header)
        return path

    def get_current_buffer_path_for_group(self, key):
        if self.grouping_info[key]['group_file']:
            path = self.grouping_info[key]['group_file'][-1]
        else:
            path = self.create_new_group_file(key)
        return path

    def create_new_group_path_for_key(self, key):
        new_buffer_path = self._get_new_path_name(key)
        self.grouping_info.add_path_to_group(key, new_buffer_path)
        with open(new_buffer_path, 'w'):
            pass
        return new_buffer_path

    def _get_new_path_name(self, key):
        filename = '{}.{}'.format(uuid.uuid4(), self.file_extension)
        return os.path.join(self.tmp_folder, filename)


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
        self.grouping_info.ensure_group_info(key)
        self.items_group_files.add_item_to_file(item, key)

    def finish_buffer_write(self, key):
        self.items_group_files.end_group_file(key)

    def pack_buffer(self, key):
        """Prepare current buffer file for group of given key to be written
        (by compressing and gathering size statistics).
        """
        self.finish_buffer_write(key)
        path = self.items_group_files.get_current_buffer_path_for_group(key)
        compressed_path = path + '.' + self.compression_format
        compress_func = get_compress_func(self.compression_format)
        compressed_hash = None

        with open(path) as source_file, open(compressed_path, 'wb') as dump_file:
            if self.hash_algorithm:
                dump_file = HashFile(dump_file, self.hash_algorithm)

            compress_func(dump_file, source_file)

            if self.hash_algorithm:
                compressed_hash = dump_file.hash.hexdigest()

        compressed_size = os.path.getsize(compressed_path)
        write_info = {
            'number_of_records': self.grouping_info[key]['buffered_items'],
            'path': path,
            'compressed_path': compressed_path,
            'size': compressed_size,
            'compressed_hash': compressed_hash,
        }
        self.metadata[compressed_path] = write_info
        return write_info

    def add_new_buffer_for_group(self, key):
        self.items_group_files.create_new_group_file(key)

    def clean_tmp_files(self, write_info):
        remove_if_exists(write_info.get('path'))
        remove_if_exists(write_info.get('compressed_path'))

    def should_write_buffer(self, key):
        if self.size_per_buffer_write and os.path.getsize(
                self.grouping_info[key]['group_file'][-1]) >= self.size_per_buffer_write:
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
