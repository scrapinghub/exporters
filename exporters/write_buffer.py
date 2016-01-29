from UserDict import UserDict
from collections import Counter
import gzip
import os
import shutil
import tempfile
import uuid
import errno


class GroupingInfo(UserDict):

    def _init_group_info_key(self, key):
        self[key] = {}
        self[key]['membership'] = key
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


class ItemsGroupFilesHandler(object):

    def __init__(self):
        self.grouping_info = GroupingInfo()
        self.file_extension = None
        self.header_line = None
        self.tmp_folder = tempfile.mkdtemp()

    def get_group_path(self, key):
        if self.grouping_info[key]['group_file']:
            path = self.grouping_info[key]['group_file'][-1]
        else:
            path = self._get_new_path_name()
            with open(path, 'w') as f:
                if self.header_line:
                    f.write(self.header_line + '\n')
            self.grouping_info.add_path_to_group(key, path)
        return path

    def _get_new_path_name(self):
        return os.path.join(self.tmp_folder,
                            '%s.%s' % (uuid.uuid4(), self.file_extension))

    def create_new_buffer_path_for_key(self, key):
        new_buffer_path = self._get_new_path_name()
        self.grouping_info.add_path_to_group(key, new_buffer_path)
        f = open(new_buffer_path, 'w')
        f.close()

    def compress_key_path(self, key):
        path = self.get_group_path(key)
        compressed_path = self._compress_file(path)
        compressed_size = os.path.getsize(compressed_path)
        write_info = {'number_of_records': self.grouping_info[key]['buffered_items'],
                      'size': compressed_size, 'compressed_path': compressed_path}
        return write_info

    def _compress_file(self, path):
        compressed_path = path + '.gz'
        with gzip.open(compressed_path, 'wb') as dump_file, open(path) as fl:
            shutil.copyfileobj(fl, dump_file)
        return compressed_path

    def get_grouping_info(self):
        return self.grouping_info

    def _silent_remove(self, filename):
        try:
            os.remove(filename)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise

    def clean_tmp_files(self, path, compressed_path):
        self._silent_remove(path)
        self._silent_remove(compressed_path)

    def close(self):
        shutil.rmtree(self.tmp_folder, ignore_errors=True)

    def add_item_to_file(self, item, key):
        path = self.get_group_path(key)
        with open(path, 'a') as f:
            f.write(item.formatted + '\n')
        self.grouping_info.add_to_group(key)

    def create_new_buffer_file(self, key, compressed_path):
        path = self.get_group_path(key)
        self.create_new_buffer_path_for_key(key)
        self.grouping_info.reset_key(key)
        self.clean_tmp_files(path, compressed_path)


class WriteBuffer(object):
    def __init__(self, items_per_buffer_write, size_per_buffer_write):

        self.files = []
        self.items_group_files = ItemsGroupFilesHandler()
        self.items_per_buffer_write = items_per_buffer_write
        self.size_per_buffer_write = size_per_buffer_write
        self.stats = {'written_items': 0}

    def buffer(self, item):
        """
        Receive an item and write it.
        """
        key = self.get_key_from_item(item)
        self.grouping_info.ensure_group_info(key)
        self.items_group_files.add_item_to_file(item, key)
        self.stats['written_items'] += 1

    def finish_buffer_write(self, key, compressed_path):
        self.items_group_files.create_new_buffer_file(key, compressed_path)

    def pack_buffer(self, key):
        return self.items_group_files.compress_key_path(key)

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
        return self.items_group_files.get_grouping_info()
