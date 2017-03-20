import os

from exporters.utils import remove_if_exists
from exporters.pipeline.base_pipeline_item import BasePipelineItem
from exporters.writers.filebase_base_writer import FilebasedGroupingBufferFilesTracker

from .utils import hash_for_file
from .grouping import GroupingBufferFilesTracker


class WriteBuffer(BasePipelineItem):

    group_files_tracker_class = GroupingBufferFilesTracker
    filebased_group_files_tracker_class = FilebasedGroupingBufferFilesTracker
    supported_options = {
    }

    def __init__(self, options, metadata, *args, **kwargs):
        super(WriteBuffer, self).__init__(options, metadata, *args, **kwargs)
        self.check_options()
        self.files = []
        self.items_per_buffer_write = kwargs['items_per_buffer_write']
        self.size_per_buffer_write = kwargs['size_per_buffer_write']
        self.hash_algorithm = kwargs.get('hash_algorithm')
        self.items_group_files = kwargs['items_group_files_handler']
        self.compression_format = kwargs.get('compression_format', 'gz')
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
        self.set_metadata_for_file(file_path, **write_info)
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

    def set_metadata(self, key, value, module='write_buffer'):
        super(WriteBuffer, self).set_metadata(key, value, module)

    def get_metadata(self, key, module='write_buffer'):
        return super(WriteBuffer, self).get_metadata(key, module) or {}

    def get_all_metadata(self, module='write_buffer'):
        return super(WriteBuffer, self).get_all_metadata(module)

    def set_metadata_for_file(self, file_name, **kwargs):
        if file_name not in self.get_all_metadata():
            self.set_metadata(file_name, kwargs)
        else:
            self.get_metadata(file_name).update(**kwargs)

    def get_metadata_for_file(self, file_name, key):
        file_meta = self.get_metadata(file_name)
        return file_meta.get(key) if file_meta else None
