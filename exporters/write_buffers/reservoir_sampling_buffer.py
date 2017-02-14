from random import randint

from exporters.write_buffers.base_buffer import (BufferFile, BaseBuffer,
                                                 GroupingBufferFilesTracker, GroupingInfo)


class ReservoirSamplingGroupingInfo(GroupingInfo):
    """ Overrides GroupingInfo class adding samples limit to
    buffered items calculation.
    """

    def __init__(self, dict=None, sample_size=1000):
        self.data = {}
        if dict is not None:
            self.update(dict)
        self.sample_size = sample_size

    def add_to_group(self, key):
        self[key]['total_items'] += 1
        buffered_items = self[key]['buffered_items']
        if buffered_items <= self.sample_size:
            buffered_items += 1
        self[key]['buffered_items'] = buffered_items


class InMemoryBufferFile(BufferFile):

    def __init__(self, formatter, tmp_folder, compression_format, sample_size,
                 file_name=None, hash_algorithm='md5'):
        self.formatter = formatter
        self.tmp_folder = tmp_folder
        self.file_extension = formatter.file_extension
        self.compression_format = compression_format
        self.path = self._get_new_path_name(file_name)
        self.sample_size = sample_size
        self.items = []

    def add_item_to_file(self, item, position):
        if position >= self.sample_size:
            return

        if len(self.items) < self.sample_size:
            self.items.append(item)
        else:
            self.items[position] = item

    def add_item_separator_to_file(self):
        pass

    def _dump_items_to_file(self):
        self.file = self._create_file()
        header = self.formatter.format_header()
        if header:
            self.file.write(header)

        for item in self.items:
            self.file.write(self.formatter.format(item))
            self.file.write(self.formatter.item_separator)

        footer = self.formatter.format_footer()
        if footer:
            self.file.write(footer)
        self.file.close()

    def end_file(self):
        self._dump_items_to_file()


class ReservoirSamplingGroupingBufferFilesTracker(GroupingBufferFilesTracker):
    """Class overrides GroupingBufferFilesTracker, which responsible for tracking buffer files.
    It implements reservoir sampling logic to randomly choose items positions in resut file.
    """

    def __init__(self, sample_size=1000, *args, **kwargs):
        super(ReservoirSamplingGroupingBufferFilesTracker, self).__init__(*args, **kwargs)

        self.grouping_info = ReservoirSamplingGroupingInfo()
        self.sample_size = sample_size

    def add_item_to_file(self, item, key):
        buffer_file = self.get_current_buffer_file_for_group(key)

        count = self.grouping_info[key]['total_items']
        if count + 1 <= self.sample_size:
            position = count
        else:
            position = randint(0, count)

        buffer_file.add_item_to_file(item, position)
        self.grouping_info.add_to_group(key)

    def _create_buffer_file(self, file_name=None):
        return InMemoryBufferFile(self.formatter, self.tmp_folder,
                                  self.compression_format, self.sample_size, file_name=file_name)


class ReservoirSamplingWriteBuffer(BaseBuffer):

    def _items_group_files_handler(self):
        return ReservoirSamplingGroupingBufferFilesTracker(
                                                         sample_size=self.items_per_buffer_write,
                                                         formatter=self.formatter,
                                                         compression_format=self.compression_format)

    def should_write_buffer(self, key):
        return False
