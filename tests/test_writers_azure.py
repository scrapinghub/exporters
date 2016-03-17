import mock
import unittest
import warnings
from exporters.records.base_record import BaseRecord
from exporters.writers.azure_blob_writer import AzureBlobWriter
from exporters.export_formatter.json_export_formatter import JsonExportFormatter
from exporters.writers.azure_file_writer import AzureFileWriter
from exporters.writers.base_writer import InconsistentWriteState


class AzureBlobWriterTest(unittest.TestCase):

    def get_writer_config(self):
        return {
            'name': 'exporters.writers.azure_blob_writer.AzureBlobWriter',
            'options': {
                'container': 'datasetsscrapinghub',
                'account_name': 'account_name',
                'account_key': 'account_key'
            }
        }

    def get_batch(self):
        data = [
            {'name': 'Roberto', 'birthday': '12/05/1987'},
            {'name': 'Claudia', 'birthday': '21/12/1985'},
        ]
        return [BaseRecord(d) for d in data]

    @mock.patch('azure.storage.blob.BlobService.create_container')
    def test_invalid_container_name(self, mock_container):
        options = self.get_writer_config()
        options['options']['container'] = 'invalid--container--name'
        warnings.simplefilter('always')
        with warnings.catch_warnings(record=True) as w:
            AzureBlobWriter(options, export_formatter=JsonExportFormatter(dict()))
            self.assertIn("Container name invalid--container--name doesn't conform",
                          str(w[0].message))

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
                'account_name': 'account_name',
                'account_key': 'account_key',
                'filebase': '/test/'
            }
        }

    def get_batch(self):
        data = [
            {'name': 'Roberto', 'birthday': '12/05/1987'},
            {'name': 'Claudia', 'birthday': '21/12/1985'},
        ]
        return [BaseRecord(d) for d in data]

    @mock.patch('azure.storage.file.FileService.get_file_properties')
    @mock.patch('azure.storage.file.FileService.put_file_from_path')
    @mock.patch('azure.storage.file.FileService.create_share')
    @mock.patch('azure.storage.file.FileService.create_directory')
    def test_write_file_consistency(self, create_mock, create_share_mock, put_file_from_path_mock, get_file_properties_mock):

        # given
        items_to_write = self.get_batch()
        options = self.get_writer_config()
        options['options']['check_consistency'] = True

        fake_properties = {
            'content-length': 999
        }

        get_file_properties_mock.return_value = fake_properties

        # when:
        writer = AzureFileWriter(options, export_formatter=JsonExportFormatter(dict()))
        try:
            writer.write_batch(items_to_write)
            writer.flush()
        finally:
            writer.close()

        with self.assertRaises(InconsistentWriteState):
                writer.finish_writing()
