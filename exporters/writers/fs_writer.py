import os
import shutil
import datetime
import re
from retrying import retry
from exporters.writers.base_writer import BaseWriter


class FSWriter(BaseWriter):
    """
    Writes items to local file system.

    Needed parameters:

        - tmp_folder (str)
            Path to store temp files.

        - filebase (str)
            Final path of items file.
    """

    parameters = {
        'filebase': {'type': basestring, 'default': '.'}
    }

    def __init__(self, options):
        super(FSWriter, self).__init__(options)
        self.prefix = self.read_option('filebase').format(datetime.datetime.now())
        self.logger.info('FSWriter has been initiated. Writing to: {}'.format(self.prefix))

    def _create_path_if_not_exist(self, path):
        if not os.path.exists(path):
            os.makedirs(path)

    @retry(wait_exponential_multiplier=500, wait_exponential_max=10000, stop_max_attempt_number=10)
    def write(self, dump_path, group_key):
        normalized = [re.sub('\W', '_', s) for s in group_key]
        target_path = os.path.join(self.prefix, os.path.sep.join(normalized))
        self._create_path_if_not_exist(target_path)
        number_of_files = len(os.listdir(target_path))
        shutil.move(dump_path, os.path.join(target_path, 'predump_{}.gz'.format(number_of_files)))
        self.logger.debug('Saved {}'.format(dump_path))
