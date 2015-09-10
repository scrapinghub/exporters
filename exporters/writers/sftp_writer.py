import os
import datetime
import re
from retrying import retry
from exporters.writers.base_writer import BaseWriter
import uuid
import pysftp


class SFTPWriter(BaseWriter):
    """
    Writes items to SFTP server.

    Needed parameters:

        - host (str)
            SFtp server ip

        - port (int)
            SFtp port

        - sftp_user (str)
            SFtp user

        - sftp_password (str)
            SFtp password

        - filebase (str)
            Base path to store the items in.
    """
    requirements = {
        'host': {'type': basestring, 'required': True},
        'port': {'type': int, 'required': True},
        'sftp_user': {'type': basestring, 'required': True},
        'sftp_password': {'type': basestring, 'required': True},
        'filebase': {'type': basestring, 'required': True}
    }

    def __init__(self, options, settings):
        super(SFTPWriter, self).__init__(options, settings)
        self.sftp_host = self.read_option('host')
        self.sftp_port = self.read_option('port')
        self.sftp_user = self.read_option('sftp_user')
        self.sftp_password = self.read_option('sftp_password')
        self.filebase = self.read_option('filebase').format(datetime.datetime.now())
        self.logger.info(
            'SFTPWriter has been initiated. host: {}. port: {}. filebase: {}'.format(self.sftp_host, self.sftp_port,
                                                                                     self.filebase))

    @retry(wait_exponential_multiplier=500, wait_exponential_max=10000, stop_max_attempt_number=10)
    def write(self, dump_path, group_key=None):
        if group_key is None:
            group_key = []
        normalized = [re.sub('\W', '_', s) for s in group_key]
        destination_path = os.path.join(self.filebase, os.path.sep.join(normalized))
        self.logger.debug('Uploading predump file')
        with pysftp.Connection(self.sftp_host, port=self.sftp_port, username=self.sftp_user,
                               password=self.sftp_password) as sftp:
            if not sftp.exists(destination_path):
                sftp.makedirs(destination_path)
            sftp.put(dump_path, destination_path + '/predump_{}.gz'.format(uuid.uuid4()))
        self.logger.debug('Saved {}'.format(dump_path))
