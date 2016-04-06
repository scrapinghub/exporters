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


class CustomNameItemsGroupFilesHandler(ItemsGroupFilesHandler):

    def __init__(self, formatter, base_filename, start_file_count=0):
        super(CustomNameItemsGroupFilesHandler, self).__init__(formatter)
        self.base_filename = self._format_date(base_filename)
        self.start_file_count = start_file_count
        self.file_count = {}

    def _get_new_path_name(self, key):
        if key not in self.file_count:
            self.file_count[key] = self.start_file_count
        name = self.base_filename.format(file_number=self.file_count[key], groups=key)
        if name == self.base_filename:
            name += '{:04d}'.format(self.file_count[key])
        filename = '{}.{}'.format(name, self.file_extension)
        self.file_count[key] += 1
        return os.path.join(self.tmp_folder, filename)

    def _format_date(self, value):
        date = datetime.datetime.now()
        return date.strftime(value)


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
        self.filebase = self.get_date_formatted_file_path()
        self.set_metadata('effective_filebase', self.filebase)
        self.written_files = {}
        self.md5_file_name = None
        self.last_written_file = None
        self.generate_md5 = self.read_option('generate_md5')

    def _items_group_files_handler(self):
        _, filename = os.path.split(self.read_option('filebase'))
        start_file_count = self.read_option('start_file_count')
        return CustomNameItemsGroupFilesHandler(self.export_formatter,
                                                filename,
                                                start_file_count)

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

    def get_date_formatted_file_path(self):
        self.logger.debug('Extracting path from filebase option')
        filebase = self.read_option('filebase')
        filebase = datetime.datetime.now().strftime(filebase)
        return filebase

    def create_filebase_name(self, group_info, extension='gz', file_name=None):
        """
        Returns filebase and file valid name
        """
        normalized = [re.sub('\W', '_', s) for s in group_info]
        filebase, prefix = os.path.split(self.filebase)
        try:
            filebase = filebase.format(groups=normalized)
        except KeyError as e:
            raise KeyError('filebase option should not contain {} key'.format(str(e)))
        if not file_name:
            file_name = prefix + '.' + extension
        return filebase, file_name

    def _get_md5(self, path):
        with open(path, 'r') as f:
            return md5_for_file(f)

    def _write(self, key):
        write_info = self.write_buffer.pack_buffer(key)
        compressed_path = write_info.get('compressed_path')
        self.write(compressed_path,
                   self.write_buffer.grouping_info[key]['membership'],
                   os.path.basename(compressed_path))
        write_info['md5'] = self._get_md5(write_info.get('compressed_path'))
        self.logger.info(
            'Checksum for file {}: {}'.format(write_info['compressed_path'], write_info['md5']))
        self.written_files[self.last_written_file] = write_info
        self.write_buffer.clean_tmp_files(key, write_info.get('compressed_path'))

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
