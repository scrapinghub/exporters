import datetime
import os

from exporters.default_retries import retry_long
from exporters.progress_callback import FtpUploadProgress
from exporters.writers.filebase_base_writer import FilebaseBaseWriter


class FtpCreateDirsException(Exception):
    pass


class FTPWriter(FilebaseBaseWriter):
    """
    Writes items to FTP server. It is a File Based writer, so it has filebase
    option available

        - host (str)
            Ftp server ip

        - port (int)
            Ftp port

        - ftp_user (str)
            Ftp user

        - ftp_password (str)
            Ftp password

        - filebase (str)
            Path to store the exported files
    """
    supported_options = {
        'host': {'type': basestring},
        'ftp_user': {'type': basestring, 'env_fallback': 'EXPORTERS_FTP_USER'},
        'ftp_password': {'type': basestring, 'env_fallback': 'EXPORTERS_FTP_PASSWORD'},
        'port': {'type': int, 'default': 21},
    }

    def __init__(self, options, *args, **kwargs):
        super(FTPWriter, self).__init__(options, *args, **kwargs)

        self.ftp_host = self.read_option('host')
        self.ftp_port = self.read_option('port')
        self.ftp_user = self.read_option('ftp_user')
        self.ftp_password = self.read_option('ftp_password')
        self.logger.info(
            'FTPWriter has been initiated. host: {}. port: {}. filebase: {}'.format(
                self.ftp_host, self.ftp_port,
                self.filebase))

    def _create_target_dir_if_needed(self, target, depth_limit=20):
        """Creates the directory for the path given, recursively creating
        parent directories when needed"""

        if depth_limit <= 0:
            raise FtpCreateDirsException('Depth limit exceeded')
        if not target:
            return
        target_dir = os.path.dirname(target)
        parent_dir, dir_name = os.path.split(target_dir)

        parent_dir_ls = []
        try:
            parent_dir_ls = self.ftp.nlst(parent_dir)
        except:
            # Possibly a microsoft server
            # They throw exceptions when we try to ls non-existing folders
            pass

        parent_dir_files = [os.path.basename(d) for d in parent_dir_ls]
        if dir_name not in parent_dir_files:
            if parent_dir and target_dir != '/':
                self._create_target_dir_if_needed(target_dir, depth_limit=depth_limit - 1)

            self.logger.info('Will create dir: %s' % target)
            self.ftp.mkd(target_dir)

    def build_ftp_instance(self):
        import ftplib
        return ftplib.FTP()

    @retry_long
    def write(self, dump_path, group_key=None, file_name=None):
        if group_key is None:
            group_key = []
        filebase_path, file_name = self.create_filebase_name(group_key, file_name=file_name)
        self.logger.info('Start uploading to {}'.format(dump_path))
        self.ftp = self.build_ftp_instance()
        self.ftp.connect(self.ftp_host, self.ftp_port)
        self.ftp.login(self.ftp_user, self.ftp_password)
        destination = (filebase_path + '/' + file_name)
        self._create_target_dir_if_needed(destination)
        progress = FtpUploadProgress(self.logger)
        self.ftp.storbinary('STOR %s' % destination, open(dump_path), callback=progress)
        self.ftp.close()
        self.last_written_file = destination
        self.logger.info('Saved {}'.format(dump_path))
