import unittest
import six
from exporters.exceptions import ConfigurationError
from exporters.exporter_config import ExporterConfig, check_for_errors
from tests.utils import valid_config_with_updates, VALID_EXPORTER_CONFIG
from exporters.writers import FSWriter


class SampleSubclassWriter(FSWriter):
    supported_options = {
        'someoption': dict(type=basestring)
    }


class ConfigValidationTest(unittest.TestCase):
    def test_find_missing_sections(self):
        with self.assertRaises(ConfigurationError):
            check_for_errors({})

    def test_check_configuration(self):
        try:
            check_for_errors(VALID_EXPORTER_CONFIG)
        except Exception:
            self.fail("check_for_errors() raised Exception unexpectedly!")

    def test_validate_returns_errors(self):
        errors = check_for_errors({}, raise_exception=False)
        self.assertIsInstance(errors, dict)
        self.assertNotEqual(len(errors), 0)

    def test_missing_supported_options(self):
        config = {
            'reader': {
                'name': 'exporters.readers.random_reader.RandomReader',
                'options': {}
            },
            'writer': {
                'name': 'exporters.writers.console_writer.ConsoleWriter',
                'options': {}
            },
            'filter': {
                'name': 'exporters.filters.no_filter.NoFilter',
                'options': {}
            },
            'transform': {
                'name': 'exporters.transform.jq_transform.JQTransform',
                'options': {}
            },
            'exporter_options': {},
            'persistence': {
                'name': 'exporters.persistence.PicklePersistence',
                'options': {
                    'file_path': '/tmp'
                }
            }
        }
        with self.assertRaises(ConfigurationError) as cm:
            check_for_errors(config)

        exception = cm.exception
        expected_errors = {
            'transform': {'jq_filter': 'Option jq_filter is missing'}
            }
        self.assertEqual(expected_errors, exception.errors)

    def test_wrong_type_supported_options(self):
        config = {
            'reader': {
                'name': 'exporters.readers.random_reader.RandomReader',
                'options': {
                    'number_of_items': {},
                    'batch_size': []
                }
            },
            'writer': {
                'name': 'exporters.writers.console_writer.ConsoleWriter',
                'options': {}
            },
            'filter': {
                'name': 'exporters.filters.no_filter.NoFilter',
                'options': {}
            },
            'transform': {
                'name': 'exporters.transform.jq_transform.JQTransform',
                'options': {
                    'jq_filter': 5
                }
            },
            'exporter_options': {},
            'persistence': {
                'name': 'exporters.persistence.PicklePersistence',
                'options': {
                    'file_path': 567
                }
            }
        }
        with self.assertRaises(ConfigurationError) as cm:
            check_for_errors(config)

        exception = cm.exception
        wrong_type_msg = 'Wrong type: found %s, expected %s'
        expected_errors = {
            'reader': {
                'number_of_items': wrong_type_msg % (dict, six.integer_types),
                'batch_size': wrong_type_msg % (list, six.integer_types)},
            'transform': {
                'jq_filter': wrong_type_msg % (int, six.string_types)},
            'persistence': {
                'file_path': wrong_type_msg % (int, six.string_types)}
        }
        self.assertEqual(expected_errors, exception.errors)
        self.assertEqual(len(exception.errors), 3)
        self.assertEqual(len(exception.errors['reader']), 2)

    def test_curate_options(self):
        options = {}
        with self.assertRaises(ConfigurationError):
            ExporterConfig(options)
        options = {'reader': ''}
        with self.assertRaises(ConfigurationError):
            ExporterConfig(options)
        options = {'reader': '', 'filter': ''}
        with self.assertRaises(ConfigurationError):
            ExporterConfig(options)
        options = {'reader': '', 'filter': '', 'transform': ''}
        with self.assertRaises(ConfigurationError):
            ExporterConfig(options)
        options = {'reader': '', 'filter': '', 'transform': '', 'writer': ''}
        with self.assertRaises(ConfigurationError):
            ExporterConfig(options)
        self.assertIsInstance(ExporterConfig(VALID_EXPORTER_CONFIG),
                              ExporterConfig)

    def test_supported_and_not_supported_options(self):
        options = valid_config_with_updates({
            'writer': {
                'name': 'exporters.writers.console_writer.ConsoleWriter',
                'options': {
                    'items_limit': 1234,
                    'not_a_supported_option': 'foo'
                }
            },
        })

        with self.assertRaisesRegexp(ValueError, 'unsupported_options'):
            ExporterConfig(options)

    def test_supported_and_not_supported_options_for_subclass(self):
        mod_name = __name__ + '.SampleSubclassWriter'

        options = valid_config_with_updates({
            'writer': {
                'name': mod_name,
                'options': {
                    'filebase': 'blah',
                    'someoption': 'blah',
                    'not_supported_option': 'foo',
                }
            }
        })
        with self.assertRaisesRegexp(ValueError, 'unsupported_options'):
            ExporterConfig(options)

    def test_long_values(self):
        options = valid_config_with_updates({
            "reader": {
                "name": "exporters.readers.hubstorage_reader.HubstorageReader",
                "options": {
                    "collection_name": "asd",
                    "project_id": 2**70,  # long in PY2, int in PY3
                }
            }
        })
        ExporterConfig(options)  # should not raise

    def test_valid_formatter(self):
        options = valid_config_with_updates({
            'exporter_options': {
                "formatter": {
                    "name": "exporters.export_formatter.json_export_formatter.JsonExportFormatter"
                }
            }
        })
        ExporterConfig(options)  # should not raise

    def test_invalid_formatter(self):
        options = valid_config_with_updates({
            'exporter_options': {
                "formatter": {}
            }
        })
        with self.assertRaisesRegexp(ValueError, 'Module name is missing'):
            ExporterConfig(options)

        options = valid_config_with_updates({
            'exporter_options': {
                "formatter": {
                    "name": "invalid.module.name"
                }
            }
        })
        with self.assertRaisesRegexp(ValueError, 'No module named'):
            ExporterConfig(options)

    def test_invalid_homogeneus_list(self):
        options = valid_config_with_updates({
            'filter': {
                'name': 'exporters.filters.key_value_filters.KeyValueFilter',
                'options': {
                    "keys": ['This', 'should', 'be', 'dicts']
                }
            }
        })
        with self.assertRaisesRegexp(ValueError, 'Wrong type'):
            ExporterConfig(options)

    def test_valid_notification(self):
        options = valid_config_with_updates({
            'exporter_options': {
                "notifications": [{
                    "name": "exporters.notifications.ses_mail_notifier.SESMailNotifier",
                }]
            }
        })
        ExporterConfig(options)  # should not raise

    def test_invalid_notification(self):
        options = valid_config_with_updates({
            'exporter_options': {
                "notifications": [{}]
            }
        })
        with self.assertRaisesRegexp(ValueError, 'Module name is missing'):
            ExporterConfig(options)

        options = valid_config_with_updates({
            'exporter_options': {
                "notifications": [{
                    "name": "invalid.module.name"
                }]
            }
        })
        with self.assertRaisesRegexp(ValueError, 'No module named'):
            ExporterConfig(options)

        options = valid_config_with_updates({
            'exporter_options': {
                "notifications": [{
                    "name": "exporters.export_formatter.json_export_formatter.JsonExportFormatter",
                    "options": {
                        "unsuported_option": True
                    }
                }]
            }
        })
        with self.assertRaisesRegexp(ValueError, 'unsupported_options'):
            ExporterConfig(options)

    def test_stream_only_sections(self):
        config = valid_config_with_updates({
            "decompressor": {
                "name": "exporters.decompressors.ZLibDecompressor",
            },
            "deserializer": {
                "name": "exporters.deserializers.CSVDeserializer",
            },
        })
        with self.assertRaises(ConfigurationError) as cm:
            check_for_errors(config)

        expected_errors = {
            'decompressor': "The 'decompressor' section can only be used with a stream reader.",
            'deserializer': "The 'deserializer' section can only be used with a stream reader.",
        }
        assert expected_errors == cm.exception.errors

        config = valid_config_with_updates({
            "reader": {
                "name": "exporters.readers.fs_reader.FSReader",
                "options": {
                    "input": "."
                }
            },
            "decompressor": {
                "name": "exporters.decompressors.ZLibDecompressor",
            },
            "deserializer": {
                "name": "exporters.deserializers.CSVDeserializer",
            },
        })
        check_for_errors(config)  # should not raise
