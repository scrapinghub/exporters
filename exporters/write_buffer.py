from collections import Counter
import gzip
import os
import shutil
import tempfile
import uuid
import errno


class WriteBuffer(object):
    def __init__(self, items_per_buffer_write, size_per_buffer_write, items_limit):
        self.tmp_folder = tempfile.mkdtemp()
        self.files = []
        self.grouping_info = {}
        self.file_extension = None
        self.header_line = None
        self.items_per_buffer_write = items_per_buffer_write
        self.size_per_buffer_write = size_per_buffer_write
        self._init_stats()
        self.items_limit = items_limit

    def buffer(self, item):
        """
        Receive an item and write it.
        """
        key = tuple(item.group_membership)
        if key not in self.grouping_info:
            self._create_grouping_info(key)

        self._add_to_buffer(item, key)
        self._update_count(item)

    def compress_key_path(self, key):
        path = self._get_group_path(key)
        compressed_path = self._compress_file(path)
        compressed_size = os.path.getsize(compressed_path)
        write_info = {'number_of_records': self.grouping_info[key]['buffered_items'],
                      'size': compressed_size}
        self.stats['written_keys']['keys'][compressed_path] = write_info
        return compressed_path

    def finish_buffer_write(self, key, compressed_path):
        path = self._get_group_path(key)
        self._create_buffer_path_for_key(key)
        self._reset_key(key)
        self._silent_remove(path)
        self._silent_remove(compressed_path)

    def _create_grouping_info(self, key):
        self.grouping_info[key] = {}
        self.grouping_info[key]['membership'] = key
        self.grouping_info[key]['total_items'] = 0
        self.grouping_info[key]['buffered_items'] = 0
        self.grouping_info[key]['group_file'] = []

    def should_write_buffer(self, key):
        if self.size_per_buffer_write and os.path.getsize(
                self.grouping_info[key]['group_file'][-1]) >= self.size_per_buffer_write:
            return True
        buffered_items = self.grouping_info[key].get('buffered_items', 0)
        return buffered_items >= self.items_per_buffer_write

    def _get_group_path(self, key):
        if self.grouping_info[key]['group_file']:
            path = self.grouping_info[key]['group_file'][-1]
        else:
            path = self._get_new_path_name()
            with open(path, 'w') as f:
                if self.header_line:
                    f.write(self.header_line + '\n')
            self.grouping_info[key]['group_file'].append(path)
        return path

    def _silent_remove(self, filename):
        try:
            os.remove(filename)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise

    def _reset_key(self, key):
        self.grouping_info[key]['buffered_items'] = 0

    def _update_count(self, item):
        for key in item:
            self.stats['written_keys']['occurrences'][key] += 1

    def _compress_file(self, path):
        compressed_path = path + '.gz'
        with gzip.open(compressed_path, 'wb') as dump_file, open(path) as fl:
            shutil.copyfileobj(fl, dump_file)
        return compressed_path

    def _create_buffer_path_for_key(self, key):
        new_buffer_path = self._get_new_path_name()
        self.grouping_info[key]['group_file'].append(new_buffer_path)
        f = open(new_buffer_path, 'w')
        f.close()

    def _get_new_path_name(self):
        return os.path.join(self.tmp_folder,
                            '%s.%s' % (uuid.uuid4(), self.file_extension))

    def _add_to_buffer(self, item, key):
        path = self._get_group_path(key)
        with open(path, 'a') as f:
            f.write(item.formatted + '\n')
        self.grouping_info[key]['total_items'] += 1
        self.grouping_info[key]['buffered_items'] += 1

    def close(self):
        shutil.rmtree(self.tmp_folder, ignore_errors=True)

    def _init_stats(self):
        self.stats = {}
        self.stats['written_keys'] = {}
        self.stats['written_keys']['keys'] = {}
        self.stats['written_keys']['occurrences'] = Counter()

