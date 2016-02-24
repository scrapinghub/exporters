import json
import os
import shutil
import tempfile
from exporters.default_retries import retry_long
from exporters.writers.filebase_base_writer import FilebaseBaseWriter


class GDriveWriter(FilebaseBaseWriter):
    """
    Writes items to Google Drive account. It is a File Based writer, so it has filebase

        - client_secret (str)
            Path to secret file

        - credentials (str)
            Path to credentials file

        - filebase (str)
            Path to store the exported files
    """

    supported_options = {
        'credentials': {'type': object},
        'client_secret': {'type': object},
    }

    def __init__(self, options, *args, **kwargs):
        super(GDriveWriter, self).__init__(options, *args, **kwargs)
        from pydrive.auth import GoogleAuth
        from pydrive.drive import GoogleDrive
        gauth = GoogleAuth()
        files_tmp_path = tempfile.mkdtemp()
        client_secret_file = os.path.join(files_tmp_path, 'secret.json')
        with open(client_secret_file, 'w') as f:
            f.write(json.dumps(self.read_option('client_secret')))
        gauth.LoadClientConfigFile(client_secret_file)
        credentials_file = os.path.join(files_tmp_path, 'credentials.json')
        with open(credentials_file, 'w') as f:
            f.write(json.dumps(self.read_option('credentials')))
        gauth.LoadCredentialsFile(credentials_file)
        shutil.rmtree(files_tmp_path)
        self.drive = GoogleDrive(gauth)
        self.logger.info(
            'GDriveWriter has been initiated. Writing to: {}'.format(self.filebase))


    def get_file_suffix(self, path, prefix):
        """
        Gets a valid filename
        """
        parent = self._ensure_folder_path(path)

        file_list = self.drive.ListFile({'q': "'{}' in parents and trashed=false and title contains '{}'".format(
                                            parent['id'], prefix)}).GetList()
        try:
            number_of_files = len(file_list)
        except:
            number_of_files = 0
        return '{0:04}'.format(number_of_files)

    def _ensure_folder_path(self, filebase_path):
        """Creates the directory for the path given, recursively creating
        parent directories when needed"""
        folders = filebase_path.split('/')
        parent = {"id": "root"}
        for folder in folders:
            file_list = self.drive.ListFile({'q': "'{}' in parents and trashed=false and title = '{}'".format(
                                                    parent['id'], folder)}).GetList()
            if not len(file_list):
                f = self.drive.CreateFile({'title': folder, 'parents': [parent],
                                           'mimeType': 'application/vnd.google-apps.folder'})
                f.Upload()
            else:
                parent = {"id": file_list[-1]["id"]}
        return parent

    @retry_long
    def write(self, dump_path, group_key=None):
        if group_key is None:
            group_key = []
        filebase_path, filename = self.create_filebase_name(group_key)
        parent = self._ensure_folder_path(filebase_path)
        file = self.drive.CreateFile({'title': filename, 'parents': [parent]})
        file.SetContentFile(dump_path)
        file.Upload()
        self.logger.info('Uploaded file {}'.format(file['title']))
