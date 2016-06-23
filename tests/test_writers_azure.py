import mock
import unittest
import warnings

from ozzy.meta import ExportMeta
from ozzy.records.base_record import BaseRecord
from ozzy.writers.azure_blob_writer import AzureBlobWriter
from ozzy.writers.azure_file_writer import AzureFileWriter
from ozzy.writers.base_writer import InconsistentWriteState

from .utils import meta


class AzureBlobWriterTest(unittest.TestCase):

    def get_writer_config(self):
        return {
            'name': 'ozzy.writers.azure_blob_writer.AzureBlobWriter',
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

    @mock.patch('azure.storage.blob.BlockBlobService.create_container')
    def test_invalid_container_name(self, mock_container):
        options = self.get_writer_config()
        options['options']['container'] = 'invalid--container--name'
        warnings.simplefilter('always')
        with warnings.catch_warnings(record=True) as w:
            writer = AzureBlobWriter(options, meta())
            self.assertIn("Container name invalid--container--name doesn't conform",
                          str(w[0].message))
            writer.close()

    @mock.patch('azure.storage.blob.BlockBlobService.create_container')
    @mock.patch('azure.storage.blob.BlockBlobService.create_blob_from_path')
    def test_write_blob(self, create_mock, put_block_blob_mock):

        # given
        items_to_write = self.get_batch()
        options = self.get_writer_config()

        # when:
        writer = AzureBlobWriter(options, meta())
        try:
            writer.write_batch(items_to_write)
            writer.flush()
        finally:
            writer.close()

        # then:
        self.assertEqual(writer.get_metadata('items_count'), 2)

    @mock.patch('azure.storage.blob.BlockBlobService.get_blob_properties')
    @mock.patch('azure.storage.blob.BlockBlobService.create_blob_from_path')
    @mock.patch('azure.storage.blob.BlockBlobService.create_container')
    def test_write_blob_consistency_size(self, create_mock, put_blob_from_path_mock,
                                         get_blob_properties_mock):
        from azure.storage.blob.models import Blob, BlobProperties

        # given
        items_to_write = self.get_batch()
        options = self.get_writer_config()
        options['options']['check_consistency'] = True

        fake_properties = BlobProperties()
        fake_properties.content_length = 999
        get_blob_properties_mock.return_value = Blob(props=fake_properties)

        # when:
        writer = AzureBlobWriter(options, meta())
        try:
            writer.write_batch(items_to_write)
            writer.flush()
        finally:
            writer.close()

        with self.assertRaisesRegexp(InconsistentWriteState, 'has unexpected size'):
            writer.finish_writing()

    @mock.patch('azure.storage.blob.BlockBlobService.get_blob_properties')
    @mock.patch('azure.storage.blob.BlockBlobService.create_blob_from_path')
    @mock.patch('azure.storage.blob.BlockBlobService.create_container')
    def test_write_blob_consistency_present(self, create_mock, create_blob_from_path_mock,
                                            get_blob_properties_mock):
        from azure.common import AzureMissingResourceHttpError
        # given
        items_to_write = self.get_batch()
        options = self.get_writer_config()
        options['options']['check_consistency'] = True

        get_blob_properties_mock.side_effect = AzureMissingResourceHttpError('', 404)

        # when:
        writer = AzureBlobWriter(options, meta())
        try:
            writer.write_batch(items_to_write)
            writer.flush()
        finally:
            writer.close()

        with self.assertRaisesRegexp(InconsistentWriteState, 'Missing blob'):
            writer.finish_writing()


class AzureFileWriterTest(unittest.TestCase):

    def get_writer_config(self):
        return {
            'name': 'ozzy.writers.azure_file_writer.AzureFileWriter',
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
    @mock.patch('azure.storage.file.FileService.create_file_from_path')
    @mock.patch('azure.storage.file.FileService.create_share')
    @mock.patch('azure.storage.file.FileService.create_directory')
    def test_write_file_consistency_size(self, create_mock, create_share_mock,
                                         put_file_from_path_mock, get_file_properties_mock):
        from azure.storage.file.models import File, FileProperties

        # given
        items_to_write = self.get_batch()
        options = self.get_writer_config()
        options['options']['check_consistency'] = True
        fake_properties = FileProperties()
        fake_properties.content_length = 999
        get_file_properties_mock.return_value = File(props=fake_properties)

        # when:
        writer = AzureFileWriter(options, ExportMeta(options))
        try:
            writer.write_batch(items_to_write)
            writer.flush()
        finally:
            writer.close()

        with self.assertRaises(InconsistentWriteState):
            writer.finish_writing()

    @mock.patch('azure.storage.file.FileService.get_file_properties')
    @mock.patch('azure.storage.file.FileService.create_file_from_path')
    @mock.patch('azure.storage.file.FileService.create_share')
    @mock.patch('azure.storage.file.FileService.create_directory')
    def test_write_file_consistency_present(self, create_mock, create_share_mock,
                                            put_file_from_path_mock, get_file_properties_mock):
        from azure.common import AzureMissingResourceHttpError
        # given
        items_to_write = self.get_batch()
        options = self.get_writer_config()
        options['options']['check_consistency'] = True

        get_file_properties_mock.side_effect = AzureMissingResourceHttpError('', 404)

        # when:
        writer = AzureFileWriter(options, meta())
        try:
            writer.write_batch(items_to_write)
            writer.flush()
        finally:
            writer.close()

        with self.assertRaisesRegexp(InconsistentWriteState, 'Missing file'):
            writer.finish_writing()

        # then:
        self.assertEqual(writer.get_metadata('items_count'), 2)
