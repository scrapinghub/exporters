import os
from retrying import retry
from exporters.writers.filebase_base_writer import FilebaseBaseWriter


class GDriveWriter(FilebaseBaseWriter):
    """
    Writes items to local file system.

        - client_secret_path (str)
            Path to secret file.

        - credentials_path (str)
            Path to credentials file.

        - filebase (str)
            Final path of items file.
    """

    supported_options = {
        'credentials_path': {'type': basestring, 'default': './gdrive-credentials.json'},
        'client_secret_path': {'type': basestring},
    }

    def __init__(self, options):
        super(GDriveWriter, self).__init__(options)
        from pydrive.auth import GoogleAuth
        from pydrive.drive import GoogleDrive
        if not os.path.exists(self.read_option('credentials_path')):
            self.logger.error(
                'A valid credentials file must be provided. Please run python bin/get_gdrive_credentials.py to get it.')
        gauth = GoogleAuth()
        gauth.LoadClientConfigFile(self.read_option('client_secret_path'))
        gauth.LoadCredentialsFile(self.read_option('credentials_path'))
        self.drive = GoogleDrive(gauth)
        self.logger.info(
            'GDriveWriter has been initiated. Writing to: {}'.format(self.filebase))

    def _create_path_if_not_exist(self, path):
        if not os.path.exists(path):
            os.makedirs(path)

    def get_file_suffix(self, path, prefix):
        parent = self._ensure_folder_path(path)

        file_list = self.drive.ListFile({'q': "'{}' in parents and trashed=false and title contains '{}'".format(
                                            parent['id'], prefix)}).GetList()
        try:
            number_of_files = len(file_list)
        except:
            number_of_files = 0
        return '{0:04}'.format(number_of_files)

    def _ensure_folder_path(self, filebase_path):
        folders = filebase_path.split('/')
        parent = {"id": "root"}
        for folder in folders:
            file_list = self.drive.ListFile({
                                                'q': "'{}' in parents and trashed=false and title = '{}'".format(
                                                    parent['id'], folder)}).GetList()
            if not len(file_list):
                f = self.drive.CreateFile({'title': folder, 'parents': [parent],
                                           'mimeType': 'application/vnd.google-apps.folder'})
                f.Upload()
            else:
                parent = {"id": file_list[-1]["id"]}
        return parent

    @retry(wait_exponential_multiplier=500, wait_exponential_max=10000,
           stop_max_attempt_number=10)
    def write(self, dump_path, group_key=None):
        if group_key is None:
            group_key = []
        filebase_path, filename = self.create_filebase_name(group_key)
        parent = self._ensure_folder_path(filebase_path)
        file = self.drive.CreateFile({'title': filename, 'parents': [parent]})
        file.SetContentFile(dump_path)
        file.Upload()
        self.logger.info('Uploaded file {}'.format(file['title']))
