import os
import shutil
import datetime
import re
from retrying import retry
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
            'FSWriter has been initiated. Writing to: {}'.format(self.filebase_path))

    def _create_path_if_not_exist(self, path):
        if not os.path.exists(path):
            os.makedirs(path)

    @retry(wait_exponential_multiplier=500, wait_exponential_max=10000, stop_max_attempt_number=10)
    def write(self, dump_path, group_key):
        normalized = [re.sub('\W', '_', s) for s in group_key]
        target_path = os.path.join(self.filebase_path, os.path.sep.join(normalized))
        self._create_path_if_not_exist(target_path)
        number_of_files = len(os.listdir(target_path))
        shutil.move(dump_path, os.path.join(target_path,
                                            '{}_{}.gz'.format(self.filenames_prefix,
                                                              number_of_files)))
        self.logger.debug('Saved {}'.format(dump_path))
