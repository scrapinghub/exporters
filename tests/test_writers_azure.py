import mock
import unittest
from exporters.records.base_record import BaseRecord
from exporters.writers.azure_blob_writer import AzureBlobWriter
from exporters.export_formatter.json_export_formatter import JsonExportFormatter
from exporters.writers.azure_file_writer import AzureFileWriter


class AzureBlobWriterTest(unittest.TestCase):

    def get_writer_config(self):
        return {
            'name': 'exporters.writers.azure_blob_writer.AzureBlobWriter',
            'options': {
                'container': 'datasetsscrapinghub',
                'account_name': 'account_name',
                'account_key': 'account_key',
                'check_consistency': False
            }
        }

    def get_batch(self):
        data = [
            {'name': 'Roberto', 'birthday': '12/05/1987'},
            {'name': 'Claudia', 'birthday': '21/12/1985'},
        ]
        return [BaseRecord(d) for d in data]


    @mock.patch('azure.storage.blob.BlobService.create_container')
    @mock.patch('azure.storage.blob.BlobService.put_block_blob_from_path')
    def test_write_blob(self, create_mock, put_block_blob_mock):

        # given
        items_to_write = self.get_batch()
        options = self.get_writer_config()

        # when:
        writer = AzureBlobWriter(options, export_formatter=JsonExportFormatter(dict()))
        try:
            writer.write_batch(items_to_write)
            writer.flush()
        finally:
            writer.close()

        # then:
        self.assertEqual(writer.writer_metadata['items_count'], 2)


class AzureFileWriterTest(unittest.TestCase):

    def get_writer_config(self):
        return {
            'name': 'exporters.writers.azure_file_writer.AzureFileWriter',
            'options': {
                'share': 'datasetsscrapinghub',
                'filebase': 'somefilebase',
                'account_name': 'account_name',
                'account_key': 'account_key',
                'check_consistency': False
            }
        }

    def get_batch(self):
        data = [
            {'name': 'Roberto', 'birthday': '12/05/1987'},
            {'name': 'Claudia', 'birthday': '21/12/1985'},
        ]
        return [BaseRecord(d) for d in data]


    @mock.patch('azure.storage.file.FileService.create_share')
    @mock.patch('azure.storage.file.FileService.create_directory')
    @mock.patch('azure.storage.file.FileService.put_file_from_path')
    def test_write_file(self, create_mock, create_directory_mock, put_file_mock):

        # given
        items_to_write = self.get_batch()
        options = self.get_writer_config()

        # when:
        writer = AzureFileWriter(options, export_formatter=JsonExportFormatter(dict()))
        try:
            writer.write_batch(items_to_write)
            writer.flush()
        finally:
            writer.close()

        # then:
        self.assertEqual(writer.writer_metadata['items_count'], 2)
