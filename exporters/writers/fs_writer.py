import glob
import os
import shutil

from exporters.writers.base_writer import InconsistentWriteState
from exporters.writers.filebase_base_writer import FilebaseBaseWriter


class FSWriter(FilebaseBaseWriter):
    """
    Writes items to local file system files. It is a File Based writer, so it has filebase
    option available

        - filebase (str)
            Path to store the exported files
    """

    supported_options = {

    }

    def __init__(self, options, *args, **kwargs):
        super(FSWriter, self).__init__(options, *args, **kwargs)
        self.set_metadata('files_written', [])

    def _create_path_if_not_exist(self, path):
        """
        Creates a folders path if it doesn't exist
        """
        if path and not os.path.exists(path):
            os.makedirs(path)

    def get_file_suffix(self, path, prefix):
        """
        Gets a valid filename
        """
        try:
            number_of_files = len(glob.glob(os.path.join(path, prefix) + '*'))
        except:
            number_of_files = 0
        return '{0:04}'.format(number_of_files)

    def _update_metadata(self, dump_path, destination):
        buffer_info = self.write_buffer.metadata.get(dump_path)
        file_info = {
            'filename': destination,
            'size': buffer_info.get('size'),
            'number_of_records': buffer_info.get('number_of_records')
        }
        self.get_metadata('files_written').append(file_info)

    def write(self, dump_path, group_key=None, file_name=None):
        if group_key is None:
            group_key = []
        filebase_path, file_name = self.create_filebase_name(group_key, file_name=file_name)
        destination = os.path.join(filebase_path, file_name)
        self._create_path_if_not_exist(filebase_path)
        shutil.copy(dump_path, destination)
        self.last_written_file = destination
        self.logger.info('Saved {}'.format(dump_path))
        self._update_metadata(dump_path, destination)

    def _check_write_consistency(self):
        for file_info in self.get_metadata('files_written'):
            if not os.path.isfile(file_info['filename']):
                raise InconsistentWriteState(
                    '{} file is not present at destination'.format(file_info['filename']))
            if os.path.getsize(file_info['filename']) != file_info['size']:
                raise InconsistentWriteState('Wrong size for file {}. Extected: {} - got {}'
                                             .format(file_info['filename'], file_info['size'],
                                                     os.path.getsize(file_info['filename'])))
        self.logger.info('Consistency check passed')
