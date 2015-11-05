import glob
import os
import shutil
import datetime
import re
from retrying import retry
from exporters.writers.base_writer import InconsistentWriteDetected
from exporters.writers.filebase_base_writer import FilebaseBaseWriter


class FSWriter(FilebaseBaseWriter):
    """
    Writes items to local file system.

        - tmp_folder (str)
            Path to store temp files.

        - filebase (str)
            Final path of items file.
    """

    supported_options = {

    }

    def __init__(self, options):
        super(FSWriter, self).__init__(options)
        self.logger.info(
            'FSWriter has been initiated. Writing to: {}'.format(self.filebase))

    def _create_path_if_not_exist(self, path):
        if not os.path.exists(path):
            os.makedirs(path)

    def get_file_suffix(self, path, prefix):
        try:
            number_of_files = len(glob.glob(os.path.join(path, prefix) + '*'))
        except:
            number_of_files = 0
        return '{0:04}'.format(number_of_files)

    @retry(wait_exponential_multiplier=500, wait_exponential_max=10000,
           stop_max_attempt_number=10)
    def write(self, dump_path, group_key=None):
        if group_key is None:
            group_key = []
        filebase_path, filename = self.create_filebase_name(group_key)
        self._create_path_if_not_exist(filebase_path)
        shutil.move(dump_path, os.path.join(filebase_path, filename))
        self.logger.info('Saved {}'.format(dump_path))
        return os.path.join(filebase_path, filename)

    def _check_write_consistency(self):
        for key, data in self.stats['written_keys']['keys'].iteritems():
            if not os.path.exists(data['destination']):
                raise InconsistentWriteDetected('File {} not found'.format(data['destination']))
            if os.path.getsize(data['destination']) != data['size']:
                raise InconsistentWriteDetected('File {} with wrong size'.format(data['destination']))
