import datetime
import os
from exporters.default_retries import retry_long
from exporters.export_managers.base_bypass import RequisitesNotMet
from exporters.utils import TmpFile
from .base_s3_bypass import BaseS3Bypass


class AzureFileS3Bypass(BaseS3Bypass):
    """
    Bypass executed by default when data source is an S3 bucket and data destination
    is an Azure share.
    It should be transparent to user. Conditions are:

        - S3Reader and AzureFileWriter are used on configuration.
        - No filter modules are set up.
        - No transform module is set up.
        - No grouper module is set up.
        - AzureFileWriter has not a items_limit set in configuration.
        - AzureFileWriter has default items_per_buffer_write and size_per_buffer_write per default.
    """

    def __init__(self, config, metadata):
        super(AzureFileS3Bypass, self).__init__(config, metadata)
        from azure.storage.file import FileService
        self.azure_service = FileService(
            self.read_option('writer', 'account_name'),
            self.read_option('writer', 'account_key'))
        self.share = self.read_option('writer', 'share')
        self.filebase_path = self._format_filebase_path(self.read_option('writer', 'filebase'))
        self._ensure_path(self.filebase_path)

    @classmethod
    def meets_conditions(cls, config):
        if not config.writer_options['name'].endswith('AzureFileWriter'):
            raise RequisitesNotMet
        super(AzureFileS3Bypass, cls).meets_conditions(config)

    def _format_filebase_path(self, filebase):
        filebase_with_date = datetime.datetime.now().strftime(filebase)
        # warning: we strip file prefix here, could be unexpected
        filebase_path, prefix = os.path.split(filebase_with_date)
        return filebase_path

    def _ensure_path(self, filebase):
        path = filebase.split('/')
        folders_added = []
        for sub_path in path:
            folders_added.append(sub_path)
            parent = '/'.join(folders_added)
            self.azure_service.create_directory(self.share, parent)

    @retry_long
    def _copy_s3_key(self, key):
        with TmpFile() as tmp_filename:
            file_name = key.name.split('/')[-1]
            key.get_contents_to_filename(tmp_filename)
            self.azure_service.put_file_from_path(
                self.share,
                self.filebase_path,
                file_name,
                tmp_filename,
                max_connections=5,
            )
