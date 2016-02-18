import mock
import unittest
from exporters.records.base_record import BaseRecord
from exporters.writers.azure_blob_writer import AzureBlobWriter


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
    @mock.patch('azure.storage.blob.BlobService.put_block_blob_from_path')
    def test_write_blob(self, create_mock, put_block_blob_mock):

        # given
        items_to_write = self.get_batch()
        options = self.get_writer_config()

        # when:
        try:
            writer = AzureBlobWriter(options)
            writer.write_batch(items_to_write)
            writer.flush()
        finally:
            writer.close()

        # then:
        self.assertEqual(writer.writer_metadata['items_count'], 2)


class AzureFileWriterTest(unittest.TestCase):

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
    @mock.patch('azure.storage.blob.BlobService.put_block_blob_from_path')
    def test_write_blob(self, create_mock, put_block_blob_mock):

        # given
        items_to_write = self.get_batch()
        options = self.get_writer_config()

        # when:
        try:
            writer = AzureBlobWriter(options)
            writer.write_batch(items_to_write)
            writer.flush()
        finally:
            writer.close()

        # then:
        self.assertEqual(writer.writer_metadata['items_count'], 2)