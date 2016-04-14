import datetime
import hashlib
import os
import re
import uuid

from exporters.write_buffer import ItemsGroupFilesHandler
from exporters.writers.base_writer import BaseWriter
import six

MD5_FILE_NAME = 'md5checksum.md5'


def md5_for_file(f, block_size=2**20):
    md5 = hashlib.md5()
    while True:
        data = f.read(block_size)
        if not data:
            break
        md5.update(data)
    return md5.hexdigest()


class Filebase(object):
    def __init__(self, filebase):
        self.raw_filebase = filebase
        self.date_formatted_filebase = self._get_date_formatted_filebase()
        self.dirname, self.prefix = os.path.split(self.date_formatted_filebase)

    def _get_date_formatted_filebase(self):
        return datetime.datetime.now().strftime(self.raw_filebase)

    def _has_group_info(self, filebase_string):
        return bool(re.findall('\{groups\[\d\]\}', filebase_string))

    def get_dirname_with_group_info(self, group_info):
        normalized = [re.sub('\W', '_', s) for s in group_info]
        try:
            if normalized:
                dirname = self.dirname.format(groups=normalized)
            else:
                dirname = self.dirname.format(groups=[''])
            return dirname
        except KeyError as e:
            raise KeyError('filebase option should not contain {} key'.format(str(e)))

    def has_groups_info(self):
        return self._has_group_info(self.date_formatted_filebase)

    def formatted_prefix(self, key, file_count):
        prefix_name = self.prefix.format(file_number=file_count, groups=key)
        if prefix_name == self.prefix:
            prefix_name += '{:04d}'.format(file_count)
        if key and not self.has_groups_info():
            prefix_name = '{}-{}'.format(prefix_name, ''.join(key))
        return prefix_name


class CustomNameItemsGroupFilesHandler(ItemsGroupFilesHandler):

    def _get_new_path_name(self, key):
        """Build a filename for a new file for a given group,
        considering the existing file count for it and the prefix
        configured in filebase.

        To ensure unique file names per group, it will use directories
        with unique names for buffers of the same group
        """
        group_files = self.grouping_info[key]['group_file']
        group_folder = self._get_group_folder(group_files)
        current_file_count = len(group_files) + self.start_file_count
        name_without_ext = self.filebase.formatted_prefix(key, current_file_count)
        filename = '{}.{}'.format(name_without_ext, self.file_extension)
        return os.path.join(group_folder, filename)

    def _get_group_folder(self, group_files):
        if group_files:
            return os.path.dirname(group_files[0])

        group_folder = os.path.join(self.tmp_folder, str(uuid.uuid4()))
        os.mkdir(group_folder)
        return group_folder

    def _format_date(self, value, date=None):
        if date is None:
            date = datetime.datetime.now()
        return date.strftime(value)

    def initialize(self, *args, **kwargs):
        self.filebase = kwargs.get('filebase')
        self.start_file_count = kwargs.get('start_file_count', 0)


class FilebaseBaseWriter(BaseWriter):
    """
    This writer is a base writer providing common methods to all file based writers

    - filebase
        Path to store the exported files

    """
    supported_options = {
        'filebase': {'type': six.string_types},
        'start_file_count': {'type': int, 'default': 0},
        'generate_md5': {'type': bool, 'default': False}
    }

    def __init__(self, *args, **kwargs):
        super(FilebaseBaseWriter, self).__init__(*args, **kwargs)
        self.filebase = Filebase(self.read_option('filebase'))
        self.set_metadata('effective_filebase', self.filebase.date_formatted_filebase)
        self.written_files = {}
        self.md5_file_name = None
        self.last_written_file = None
        self.generate_md5 = self.read_option('generate_md5')
        self.write_buffer.initialize_items_group_files_handler(
                filebase=self.filebase,
                start_file_count=self.read_option('start_file_count'))

    def _items_group_files_handler(self):
        return CustomNameItemsGroupFilesHandler(self.export_formatter)

    def write(self, path, key, file_name=False):
        """
        Receive path to temp dump file and group key, and write it to the proper location.
        """
        raise NotImplementedError

    def get_file_suffix(self, path, prefix):
        """
        This method is a fallback to provide valid random filenames
        """
        return str(uuid.uuid4())

    def create_filebase_name(self, group_info, extension='gz', file_name=None):
        """
        Return tuple of resolved destination folder name and file name
        """
        dirname = self.filebase.get_dirname_with_group_info(group_info)
        if not file_name:
            file_name = self.filebase.prefix + '.' + extension
        return dirname, file_name

    def _get_md5(self, path):
        with open(path, 'r') as f:
            return md5_for_file(f)

    def _write_current_buffer_for_group_key(self, key):
        write_info = self.write_buffer.pack_buffer(key)
        compressed_path = write_info.get('compressed_path')
        self.write(compressed_path,
                   self.write_buffer.grouping_info[key]['membership'],
                   file_name=os.path.basename(compressed_path))
        write_info['md5'] = self._get_md5(write_info.get('compressed_path'))
        self.logger.info(
            'Checksum for file {}: {}'.format(write_info['compressed_path'], write_info['md5']))
        self.written_files[self.last_written_file] = write_info

        self.write_buffer.clean_tmp_files(key, write_info.get('compressed_path'))
        self.write_buffer.add_new_buffer_for_group(key)

    def finish_writing(self):
        super(FilebaseBaseWriter, self).finish_writing()
        if self.generate_md5:
            try:
                with open(MD5_FILE_NAME, 'a') as f:
                    for file_name, write_info in self.written_files.iteritems():
                        write_info = self.written_files[file_name]
                        f.write('{} {}'.format(write_info['md5'], file_name)+'\n')
                self.write_buffer.set_metadata_for_file(
                    MD5_FILE_NAME, size=os.path.getsize(MD5_FILE_NAME))
                self.write(MD5_FILE_NAME, None, file_name=MD5_FILE_NAME)
            finally:
                os.remove(MD5_FILE_NAME)
