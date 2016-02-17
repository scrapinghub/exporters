import errno
import gzip
import os
import shutil
import tempfile
import uuid


class FileHandler(object):

    file_extension = None

    def __init__(self, grouping_info, export_metadata):
        self.grouping_info = grouping_info
        self.tmp_folder = tempfile.mkdtemp()
        self.export_metadata = export_metadata

    def _get_new_path_name(self):
        return os.path.join(self.tmp_folder,
                            '%s.%s' % (uuid.uuid4(), self.file_extension))

    def create_new_buffer_file(self, key, compressed_path):
        path = self.get_group_path(key)
        self.create_new_buffer_path_for_key(key)
        self.grouping_info.reset_key(key)
        self.clean_tmp_files(path, compressed_path)

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

    def get_group_path(self, key):
        if self.grouping_info[key]['group_file']:
            path = self.grouping_info[key]['group_file'][-1]
        else:
            path = self.create_new_buffer_path_for_key(key)
            self.grouping_info.add_path_to_group(key, path)

        return path

    def create_new_buffer_path_for_key(self, key):
        new_buffer_path = self._get_new_path_name()
        self.grouping_info.add_path_to_group(key, new_buffer_path)
        with open(new_buffer_path, 'w') as f:
            pass
        return new_buffer_path


class JsonFileHandler(FileHandler):

    file_extension = 'jl'


class CSVFileHandler(FileHandler):

    file_extension = 'csv'

    def create_new_buffer_path_for_key(self, key):
        new_buffer_path = self._get_new_path_name()
        self.grouping_info.add_path_to_group(key, new_buffer_path)
        with open(new_buffer_path, 'w') as f:
            if self.export_metadata.get('formatter', {}).get('header'):
                f.write(self.export_metadata.get('formatter', {}).get('header') + '\n')
        return new_buffer_path


class XMLFileHandler(FileHandler):

    file_extension = 'xml'

    def create_new_buffer_path_for_key(self, key):
        new_buffer_path = self._get_new_path_name()
        self.grouping_info.add_path_to_group(key, new_buffer_path)
        with open(new_buffer_path, 'w') as f:
            f.write(self.export_metadata.get('formatter', {}).get('header') + '\n')
        return new_buffer_path

    def _compress_file(self, path):
        with open(path, 'a') as f:
            f.write(self.export_metadata.get('formatter', {}).get('bottom'))
        return super(XMLFileHandler, self)._compress_file(path)