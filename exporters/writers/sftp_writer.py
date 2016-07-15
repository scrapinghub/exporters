import errno
import six

from exporters.default_retries import retry_long
from exporters.progress_callback import SftpUploadProgress
from exporters.writers.base_writer import InconsistentWriteState

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
        'host': {'type': six.string_types},
        'sftp_user': {'type': six.string_types, 'env_fallback': 'EXPORTERS_SFTP_USER'},
        'sftp_password': {'type': six.string_types, 'env_fallback': 'EXPORTERS_SFTP_PASSWORD'},
        'port': {'type': six.integer_types, 'default': 22},
    }

    def __init__(self, options, *args, **kwargs):
        super(SFTPWriter, self).__init__(options, *args, **kwargs)
        self.sftp_host = self.read_option('host')
        self.sftp_port = self.read_option('port')
        self.sftp_user = self.read_option('sftp_user')
        self.sftp_password = self.read_option('sftp_password')
        self.set_metadata('files_written', [])

    def _update_metadata(self, dump_path, destination):
        buffer_info = self.write_buffer.metadata.get(dump_path, {})
        file_info = {
            'filename': destination,
            'size': buffer_info.get('size'),
            'number_of_records': buffer_info.get('number_of_records')
        }
        self.get_metadata('files_written').append(file_info)

    @retry_long
    def write(self, dump_path, group_key=None, file_name=None):
        import pysftp
        if group_key is None:
            group_key = []

        filebase_path, file_name = self.create_filebase_name(group_key, file_name=file_name)
        destination = (filebase_path + '/' + file_name)
        self.logger.info('Start uploading to {}'.format(dump_path))
        with pysftp.Connection(self.sftp_host, port=self.sftp_port,
                               username=self.sftp_user,
                               password=self.sftp_password) as sftp:
            if not sftp.exists(filebase_path):
                sftp.makedirs(filebase_path)
            progress = SftpUploadProgress(self.logger)
            sftp.put(dump_path, destination, callback=progress)
        self.last_written_file = destination
        self._update_metadata(dump_path, destination)
        self.logger.info('Saved {}'.format(dump_path))

    def _check_write_consistency(self):
        import pysftp
        with pysftp.Connection(self.sftp_host, port=self.sftp_port,
                               username=self.sftp_user,
                               password=self.sftp_password) as sftp:
            for file_info in self.get_metadata('files_written'):
                try:
                    sftp_info = sftp.stat(file_info['filename'])
                except IOError as e:
                    if e.errno == errno.ENOENT:
                        raise InconsistentWriteState(
                            '{} file is not present at destination'.format(file_info['filename']))
                sftp_size = sftp_info.st_size
                if sftp_size != file_info['size']:
                    raise InconsistentWriteState('Wrong size for file {}. Expected: {} - got {}'
                                                 .format(file_info['filename'], file_info['size'],
                                                         sftp_size))
        self.logger.info('Consistency check passed')
