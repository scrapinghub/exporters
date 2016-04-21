from collections import Counter
from exporters.default_retries import retry_long
from exporters.writers.filebase_base_writer import FilebaseBaseWriter
import six


class DropboxWriter(FilebaseBaseWriter):
    """
    Writes items to dropbox folder.
    options available

        - access_token (str)
            Oauth access token for Dropbox api.

        - filebase (str)
            Base path to store the items in the share.

    """
    supported_options = {
        'access_token': {'type': six.string_types, 'env_fallback': 'EXPORTERS_DROPBOXWRITER_TOKEN'},
    }

    def __init__(self, *args, **kw):
        from dropbox import Dropbox
        super(DropboxWriter, self).__init__(*args, **kw)
        access_token = self.read_option('access_token')
        self.set_metadata('files_counter', Counter())
        self.client = Dropbox(access_token)

    def write(self, dump_path, group_key=None, file_name=False):
        if group_key is None:
            group_key = []
        self._write_file(dump_path, group_key, file_name)

    @retry_long
    def _upload_file(self, input_file, filepath):
        from dropbox import files
        session_id = self.client.files_upload_session_start('')
        current_offset = 0
        while True:
            data = input_file.read(2**20)
            if not data:
                break
            self.client.files_upload_session_append(data, session_id.session_id, current_offset)
            current_offset += len(data)
        cursor = files.UploadSessionCursor(session_id.session_id, current_offset)
        self.client.files_upload_session_finish(
            '', cursor, files.CommitInfo(path='{}'.format(filepath)))

    def _write_file(self, dump_path, group_key, file_name=None):
        filebase_path, file_name = self.create_filebase_name(group_key, file_name=file_name)
        with open(dump_path, 'r') as f:
            self._upload_file(f, '{}/{}'.format(filebase_path, file_name))
        self.get_metadata('files_counter')[filebase_path] += 1

    def get_file_suffix(self, path, prefix):
        number_of_keys = self.get_metadata('files_counter').get(path, 0)
        suffix = '{}'.format(str(number_of_keys))
        return suffix
