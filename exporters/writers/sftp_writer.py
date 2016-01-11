from exporters.default_retries import retry_long
from exporters.progress_callback import SftpUploadProgress

from exporters.writers.filebase_base_writer import FilebaseBaseWriter


class SFTPWriter(FilebaseBaseWriter):
    """
    Writes items to SFTP server. It is a File Based writer, so it has filebase
    option available

        - host (str)
            SFtp server ip

        - port (int)
            SFtp port

        - sftp_user (str)
            SFtp user

        - sftp_password (str)
            SFtp password

        - filebase (str)
            Path to store the exported files
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

    @retry_long
    def write(self, dump_path, group_key=None):
        import pysftp
        if group_key is None:
            group_key = []

        filebase_path, filename = self.create_filebase_name(group_key)
        destination = (filebase_path + '/' + filename)

        self.logger.info('Start uploading to {}'.format(dump_path))
        with pysftp.Connection(self.sftp_host, port=self.sftp_port,
                               username=self.sftp_user,
                               password=self.sftp_password) as sftp:
            if not sftp.exists(filebase_path):
                sftp.makedirs(filebase_path)
            progress = SftpUploadProgress(self.logger)
            sftp.put(dump_path, destination, callback=progress)
        self.logger.info('Saved {}'.format(dump_path))
