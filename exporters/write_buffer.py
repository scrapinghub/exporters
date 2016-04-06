import gzip
import os
import shutil
import tempfile
import uuid
from six.moves import UserDict


def _silent_remove(filename):
    import errno
    try:
        os.remove(filename)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise


class GroupingInfo(UserDict):
    """Contains groups metadata for the grouping feature in writers,
    tracking which group keys being used plus some details for each group:

    * how many items were written
    * which are the buffer files used
    * how many items are in the current buffer
    """

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
        path = self.get_group_path(key)
        with open(path, 'a') as f:
            f.write(content + '\n')
        self.grouping_info.add_to_group(key)

    def add_item_to_file(self, item, key):
        content = self.formatter.format(item)
        self._add_to_file(content, key)

    def end_group_file(self, key):
        path = self.get_group_path(key)
        footer = self.formatter.format_footer()
        if footer:
            with open(path, 'a') as f:
                f.write(footer)
        return path

    def close(self):
        shutil.rmtree(self.tmp_folder, ignore_errors=True)

    def get_grouping_info(self):
        return self.grouping_info

    def clean_tmp_files(self, compressed_path):
        path = compressed_path[:-3]
        _silent_remove(path)
        _silent_remove(compressed_path)

    def get_group_path(self, key):
        if self.grouping_info[key]['group_file']:
            path = self.grouping_info[key]['group_file'][-1]
        else:
            path = self.create_new_group_file(key)
            self.grouping_info.add_path_to_group(key, path)
        return path

    def create_new_group_file(self, key):
        path = self.create_new_group_path_for_key(key)
        self.grouping_info.reset_key(key)
        header = self.formatter.format_header()
        if header:
            with open(path, 'w') as f:
                f.write(header)
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


class WriteBuffer(object):

    def __init__(self, items_per_buffer_write, size_per_buffer_write, items_group_files_handler):
        self.files = []
        self.items_group_files = items_group_files_handler
        self.items_per_buffer_write = items_per_buffer_write
        self.size_per_buffer_write = size_per_buffer_write
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
        self.finish_buffer_write(key)
        write_info = self.items_group_files.compress_key_path(key)
        self.metadata[write_info['compressed_path']] = write_info
        self.items_group_files.create_new_group_file(key)
        return write_info

    def clean_tmp_files(self, key, compressed_path):
        self.items_group_files.clean_tmp_files(compressed_path)

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
        return self.metadata.get(buffer_path, {}).get(meta_key)

    def get_grouping_info(self):
        return self.grouping_info

    def set_metadata_for_file(self, file_name, **kwargs):
        if file_name not in self.metadata:
            self.metadata[file_name] = {}
        self.metadata[file_name].update(**kwargs)
