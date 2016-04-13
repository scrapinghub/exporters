import unittest
from exporters.bypasses.s3_to_azure_blob_bypass import AzureBlobS3Bypass
from exporters.bypasses.s3_to_azure_file_bypass import AzureFileS3Bypass
from exporters.export_managers.base_bypass import RequisitesNotMet
from exporters.exporter_config import ExporterConfig


def create_azure_file_s3_bypass_simple_config(**kwargs):
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
            'name': 'exporters.writers.azure_file_writer.AzureFileWriter',
            'options': {
                'filebase': 'bypass_test/',
                'share': 'some_share',
                'account_name': 'a',
                'account_key': 'a'
            }
        }
    }
    config.update(kwargs)
    return ExporterConfig(config)


class S3AzureFileBypassConditionsTest(unittest.TestCase):

    def test_should_meet_conditions(self):
        # shouldn't raise any exception
        AzureFileS3Bypass.meets_conditions(create_azure_file_s3_bypass_simple_config())

    def test_custom_filter_should_not_meet_conditions(self):
        # given:
        config = create_azure_file_s3_bypass_simple_config(filter={
            'name': 'exporters.filters.PythonexpFilter',
            'options': {'python_expression': 'None'}
        })

        # when:

        # then:
        with self.assertRaises(RequisitesNotMet):
            AzureFileS3Bypass.meets_conditions(config)

    def test_custom_grouper_should_not_meet_conditions(self):
        # given:
        config = create_azure_file_s3_bypass_simple_config(grouper={
            'name': 'whatever.Grouper',
        })

        # when:

        # then:
        with self.assertRaises(RequisitesNotMet):
            AzureFileS3Bypass.meets_conditions(config)

    def test_items_limit_should_not_meet_conditions(self):
        # given:
        config = create_azure_file_s3_bypass_simple_config()
        config.writer_options['options']['items_limit'] = 10

        # when:

        # then:
        with self.assertRaises(RequisitesNotMet):
            AzureFileS3Bypass.meets_conditions(config)


def create_azure_blob_s3_bypass_simple_config(**kwargs):
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
    config.update(kwargs)
    return ExporterConfig(config)


class S3AzureBlobBypassConditionsTest(unittest.TestCase):

    def test_should_meet_conditions(self):
        # shouldn't raise any exception
        AzureBlobS3Bypass.meets_conditions(create_azure_blob_s3_bypass_simple_config())

    def test_custom_filter_should_not_meet_conditions(self):
        # given:
        config = create_azure_blob_s3_bypass_simple_config(filter={
            'name': 'exporters.filters.PythonexpFilter',
            'options': {'python_expression': 'None'}
        })

        # when:

        # then:
        with self.assertRaises(RequisitesNotMet):
            AzureBlobS3Bypass.meets_conditions(config)

    def test_custom_grouper_should_not_meet_conditions(self):
        # given:
        config = create_azure_blob_s3_bypass_simple_config(grouper={
            'name': 'whatever.Grouper',
        })

        # when:

        # then:
        with self.assertRaises(RequisitesNotMet):
            AzureBlobS3Bypass.meets_conditions(config)

    def test_items_limit_should_not_meet_conditions(self):
        # given:
        config = create_azure_blob_s3_bypass_simple_config()
        config.writer_options['options']['items_limit'] = 10

        # when:

        # then:
        with self.assertRaises(RequisitesNotMet):
            AzureBlobS3Bypass.meets_conditions(config)
