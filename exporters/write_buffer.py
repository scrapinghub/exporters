import os
from UserDict import UserDict


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

    def __init__(self, extension):
        self.grouping_info = GroupingInfo()
        self.file_extension = None
        self.header_line = None
        self._set_extension(extension)

    def _set_extension(self, extension):
        self.file_extension = extension['format']
        self.file_handler = extension['file_handler'](self.grouping_info)

    def add_item_to_file(self, item, key):
        path = self.file_handler.get_group_path(key)
        with open(path, 'a') as f:
            f.write(item.formatted + '\n')
        self.grouping_info.add_to_group(key)

    def create_new_buffer_file(self, key, compressed_path):
        return self.file_handler.create_new_buffer_file(key, compressed_path)

    def close(self):
        return self.file_handler.close()

    def compress_key_path(self, key):
        return self.file_handler.compress_key_path(key)

    def get_grouping_info(self):
        return self.grouping_info


class WriteBuffer(object):

    def __init__(self, items_per_buffer_write, size_per_buffer_write, extension):
        self.files = []
        self.items_group_files = ItemsGroupFilesHandler(extension)
        self.items_per_buffer_write = items_per_buffer_write
        self.size_per_buffer_write = size_per_buffer_write
        self.stats = {'written_items': 0}
        self.metadata = {}

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
        write_info = self.items_group_files.compress_key_path(key)
        self.metadata[write_info['compressed_path']] = write_info
        return write_info

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

    def get_metadata(self, buffer_path, meta_key):
        return self.metadata[buffer_path].get(meta_key)
