from retrying import retry
from exporters.writers.base_writer import InconsistentWriteDetected

from exporters.writers.filebase_base_writer import FilebaseBaseWriter


class SFTPWriter(FilebaseBaseWriter):
    """
    Writes items to SFTP server.

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
    supported_options = {
        'host': {'type': basestring},
        'sftp_user': {'type': basestring, 'env_fallback': 'EXPORTERS_SFTP_USER'},
        'sftp_password': {'type': basestring, 'env_fallback': 'EXPORTERS_SFTP_PASSWORD'},
        'port': {'type': int, 'default': 22},
    }

    def __init__(self, options):
        super(SFTPWriter, self).__init__(options)
        self.sftp_host = self.read_option('host')
        self.sftp_port = self.read_option('port')
        self.sftp_user = self.read_option('sftp_user')
        self.sftp_password = self.read_option('sftp_password')
        self.logger.info(
            'SFTPWriter has been initiated. host: {}. port: {}. filebase: {}'.format(
                self.sftp_host, self.sftp_port,
                self.filebase))

    @retry(wait_exponential_multiplier=500, wait_exponential_max=10000,
           stop_max_attempt_number=10)
    def write(self, dump_path, group_key=None):
        import pysftp
        if group_key is None:
            group_key = []

        filebase_path, filename = self.create_filebase_name(group_key)

        self.logger.info('Start uploading to {}'.format(dump_path))
        destination_path = filebase_path + '/' + filename
        with pysftp.Connection(self.sftp_host, port=self.sftp_port,
                               username=self.sftp_user,
                               password=self.sftp_password) as sftp:
            if not sftp.exists(filebase_path):
                sftp.makedirs(filebase_path)
            sftp.put(dump_path, destination_path)
        self.logger.info('Saved {}'.format(dump_path))
        return destination_path

    def _check_write_consistency(self):
        import pysftp
        sftp = pysftp.Connection(self.sftp_host, port=self.sftp_port,
                                   username=self.sftp_user,
                                   password=self.sftp_password)
        for key, data in self.stats['written_keys']['keys'].iteritems():
                if not sftp.exists(data['destination']):
                    raise InconsistentWriteDetected('File {} not found'.format(data['destination']))
        sftp.close()