import glob
import os
import shutil
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

    def __init__(self, options):
        super(FSWriter, self).__init__(options)
        self.logger.info(
            'FSWriter has been initiated. Writing to: {}'.format(self.filebase))

    def _create_path_if_not_exist(self, path):
        """
        Creates a folders path if it doesn't exist
        """
        if not os.path.exists(path):
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

    def write(self, dump_path, group_key=None):
        if group_key is None:
            group_key = []
        filebase_path, filename = self.create_filebase_name(group_key)
        destination = os.path.join(filebase_path, filename)
        self._create_path_if_not_exist(filebase_path)
        shutil.move(dump_path, destination)
        self.logger.info('Saved {}'.format(dump_path))
