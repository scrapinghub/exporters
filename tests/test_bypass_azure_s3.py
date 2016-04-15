import boto
import mock
import moto
import unittest
from exporters.bypasses.s3_to_azure_blob_bypass import S3AzureBlobBypass
from exporters.bypasses.s3_to_azure_file_bypass import S3AzureFileBypass
from exporters.export_managers.base_bypass import RequisitesNotMet
from exporters.export_managers.basic_exporter import BasicExporter
from exporters.exporter_config import ExporterConfig

from .utils import create_s3_keys


def create_s3_azure_file_bypass_simple_opts(**kwargs):
    config = {
        'reader': {
            'name': 'exporters.readers.s3_reader.S3Reader',
            'options': {
                'bucket': 'source_bucket',
                'aws_access_key_id': 'aws-key',
                'aws_secret_access_key': 'aws-secret-key',
                'prefix': 'some_prefix/'
            }
        },
        'writer': {
            'name': 'exporters.writers.azure_file_writer.AzureFileWriter',
            'options': {
                'filebase': 'bypass_test/',
                'share': 'some_share',
                'account_name': 'azure-acc',
                'account_key': 'azure-key'
            }
        }
    }
    config.update(kwargs)
    return config


def create_s3_azure_file_bypass_simple_config(**kwargs):
    config = create_s3_azure_file_bypass_simple_opts(**kwargs)
    return ExporterConfig(config)


class S3AzureFileBypassConditionsTest(unittest.TestCase):

    def test_should_meet_conditions(self):
        # shouldn't raise any exception
        S3AzureFileBypass.meets_conditions(create_s3_azure_file_bypass_simple_config())

    def test_custom_filter_should_not_meet_conditions(self):
        # given:
        config = create_s3_azure_file_bypass_simple_config(filter={
            'name': 'exporters.filters.PythonexpFilter',
            'options': {'python_expression': 'None'}
        })

        # when:

        # then:
        with self.assertRaises(RequisitesNotMet):
            S3AzureFileBypass.meets_conditions(config)

    def test_custom_grouper_should_not_meet_conditions(self):
        # given:
        config = create_s3_azure_file_bypass_simple_config(grouper={
            'name': 'whatever.Grouper',
        })

        # when:

        # then:
        with self.assertRaises(RequisitesNotMet):
            S3AzureFileBypass.meets_conditions(config)

    def test_items_limit_should_not_meet_conditions(self):
        # given:
        config = create_s3_azure_file_bypass_simple_config()
        config.writer_options['options']['items_limit'] = 10

        # when:

        # then:
        with self.assertRaises(RequisitesNotMet):
            S3AzureFileBypass.meets_conditions(config)


class S3AzureFileBypassTest(unittest.TestCase):
    def test_bypass(self):
        # given:
        opts = create_s3_azure_file_bypass_simple_opts()

        # when:
        with moto.mock_s3(), mock.patch('azure.storage.file.FileService') as azure:
            s3_conn = boto.connect_s3()
            bucket = s3_conn.create_bucket(opts['reader']['options']['bucket'])
            keys = ['some_prefix/{}'.format(k) for k in ['some', 'keys', 'here']]
            create_s3_keys(bucket, keys)

            exporter = BasicExporter(opts)
            exporter.export()

        # then:
        self.assertEquals(exporter.writer.get_metadata('items_count'), 0,
                          "No items should be read")
        self.assertEquals(exporter.reader.get_metadata('read_items'), 0,
                          "No items should get written")
        azure_puts = [
            call for call in azure.mock_calls if call[0] == '().copy_file'
        ]
        self.assertEquals(len(azure_puts), len(keys),
                          "all keys should be put into Azure files")


def create_s3_azure_blob_bypass_simple_opts(**kwargs):
    config = {
        'reader': {
            'name': 'exporters.readers.s3_reader.S3Reader',
            'options': {
                'bucket': 'source_bucket',
                'aws_access_key_id': 'a',
                'aws_secret_access_key': 'a',
                'prefix': 'some_prefix/'
            }
        },
        'writer': {
            'name': 'exporters.writers.azure_blob_writer.AzureBlobWriter',
            'options': {
                'container': 'some_share',
                'account_name': 'a',
                'account_key': 'a'
            }
        }
    }
    config.update(**kwargs)
    return config


def create_s3_azure_blob_bypass_simple_config(**kwargs):
    config = create_s3_azure_blob_bypass_simple_opts(**kwargs)
    return ExporterConfig(config)


class S3AzureBlobBypassConditionsTest(unittest.TestCase):

    def test_should_meet_conditions(self):
        # shouldn't raise any exception
        S3AzureBlobBypass.meets_conditions(create_s3_azure_blob_bypass_simple_config())

    def test_custom_filter_should_not_meet_conditions(self):
        # given:
        config = create_s3_azure_blob_bypass_simple_config(filter={
            'name': 'exporters.filters.PythonexpFilter',
            'options': {'python_expression': 'None'}
        })

        # when:

        # then:
        with self.assertRaises(RequisitesNotMet):
            S3AzureBlobBypass.meets_conditions(config)

    def test_custom_grouper_should_not_meet_conditions(self):
        # given:
        config = create_s3_azure_blob_bypass_simple_config(grouper={
            'name': 'whatever.Grouper',
        })

        # when:

        # then:
        with self.assertRaises(RequisitesNotMet):
            S3AzureBlobBypass.meets_conditions(config)

    def test_items_limit_should_not_meet_conditions(self):
        # given:
        config = create_s3_azure_blob_bypass_simple_config()
        config.writer_options['options']['items_limit'] = 10

        # when:

        # then:
        with self.assertRaises(RequisitesNotMet):
            S3AzureBlobBypass.meets_conditions(config)


class S3AzureBlobBypassTest(unittest.TestCase):
    def test_bypass(self):
        # given:
        opts = create_s3_azure_blob_bypass_simple_opts()

        # when:
        with moto.mock_s3(), mock.patch('azure.storage.blob.BlobService') as azure:
            s3_conn = boto.connect_s3()
            bucket = s3_conn.create_bucket(opts['reader']['options']['bucket'])
            keys = ['some_prefix/{}'.format(k) for k in ['this', 'test', 'has', 'keys']]
            create_s3_keys(bucket, keys)

            exporter = BasicExporter(opts)
            exporter.export()

        # then:
        self.assertEquals(exporter.writer.get_metadata('items_count'), 0,
                          "No items should be read")
        self.assertEquals(exporter.reader.get_metadata('read_items'), 0,
                          "No items should get written")
        azure_puts = [
            call for call in azure.mock_calls if call[0] == '().copy_blob'
        ]
        self.assertEquals(len(azure_puts), len(keys),
                          "all keys should be put into Azure blobs")
