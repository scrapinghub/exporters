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

    def reset_buffered_items(self, key):
        self[key]['buffered_items'] = 0

    def add_to_group(self, key):
        self[key]['total_items'] += 1
        self[key]['buffered_items'] += 1


class ItemsGroupFiles(object):

    def __init__(self, tmp_folder):
        self.grouping_info = GroupingInfo()
        self.file_extension = None
        self.header_line = None
        self.tmp_folder = tmp_folder

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
        # self.stats['written_keys']['keys'][compressed_path] = write_info
        return write_info

    def _compress_file(self, path):
        compressed_path = path + '.gz'
        with gzip.open(compressed_path, 'wb') as dump_file, open(path) as fl:
            shutil.copyfileobj(fl, dump_file)
        return compressed_path

    def get_grouping_info(self):
        return self.grouping_info


class WriteBuffer(object):
    def __init__(self, items_per_buffer_write, size_per_buffer_write):
        self.tmp_folder = tempfile.mkdtemp()
        self.files = []
        self.items_group_files = ItemsGroupFiles(self.tmp_folder)
        self.items_per_buffer_write = items_per_buffer_write
        self.size_per_buffer_write = size_per_buffer_write
        self.stats = {'written_keys': {'keys': {}, 'occurrences': Counter()}}

    def buffer(self, item):
        """
        Receive an item and write it.
        """
        key = self.get_key_from_item(item)
        self.grouping_info.ensure_group_info(key)
        self._add_to_buffer(item, key)
        self._update_count(item)

    def finish_buffer_write(self, key, compressed_path):
        path = self.items_group_files.get_group_path(key)
        self.items_group_files.create_new_buffer_path_for_key(key)
        self._reset_key(key)
        self._silent_remove(path)
        self._silent_remove(compressed_path)

    def pack_buffer(self, key):
        write_info = self.items_group_files.compress_key_path(key)
        self.stats['written_keys']['keys'][write_info['compressed_path']] = write_info
        return write_info

    def should_write_buffer(self, key):
        if self.size_per_buffer_write and os.path.getsize(
                self.grouping_info[key]['group_file'][-1]) >= self.size_per_buffer_write:
            return True
        buffered_items = self.grouping_info[key].get('buffered_items', 0)
        return buffered_items >= self.items_per_buffer_write

    def _silent_remove(self, filename):
        try:
            os.remove(filename)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise

    def _reset_key(self, key):
        self.grouping_info.reset_buffered_items(key)

    def _update_count(self, item):
        for key in item:
            self.stats['written_keys']['occurrences'][key] += 1

    def _add_to_buffer(self, item, key):
        path = self.items_group_files.get_group_path(key)
        with open(path, 'a') as f:
            f.write(item.formatted + '\n')
        self.grouping_info.add_to_group(key)

    def close(self):
        shutil.rmtree(self.tmp_folder, ignore_errors=True)

    def get_key_from_item(self, item):
        return tuple(item.group_membership)

    @property
    def grouping_info(self):
        return self.items_group_files.get_grouping_info()
