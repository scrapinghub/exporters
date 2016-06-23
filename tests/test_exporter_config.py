import unittest
from ozzy.exceptions import ConfigurationError
from ozzy.exporter_config import ExporterConfig, check_for_errors
from tests.utils import valid_config_with_updates, VALID_EXPORTER_CONFIG
from ozzy.writers import FSWriter


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
                'name': 'ozzy.readers.random_reader.RandomReader',
                'options': {}
            },
            'writer': {
                'name': 'ozzy.writers.console_writer.ConsoleWriter',
                'options': {}
            },
            'filter': {
                'name': 'ozzy.filters.no_filter.NoFilter',
                'options': {}
            },
            'transform': {
                'name': 'ozzy.transform.jq_transform.JQTransform',
                'options': {}
            },
            'exporter_options': {},
            'persistence': {
                'name': 'ozzy.persistence.PicklePersistence',
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
                'name': 'ozzy.readers.random_reader.RandomReader',
                'options': {
                    'number_of_items': {},
                    'batch_size': []
                }
            },
            'writer': {
                'name': 'ozzy.writers.console_writer.ConsoleWriter',
                'options': {}
            },
            'filter': {
                'name': 'ozzy.filters.no_filter.NoFilter',
                'options': {}
            },
            'transform': {
                'name': 'ozzy.transform.jq_transform.JQTransform',
                'options': {
                    'jq_filter': 5
                }
            },
            'exporter_options': {},
            'persistence': {
                'name': 'ozzy.persistence.PicklePersistence',
                'options': {
                    'file_path': 567
                }
            }
        }
        with self.assertRaises(ConfigurationError) as cm:
            check_for_errors(config)

        exception = cm.exception
        expected_errors = {
            'reader': {
                'number_of_items': 'Wrong type: found <type \'dict\'>, expected <type \'int\'>',
                'batch_size': 'Wrong type: found <type \'list\'>, expected <type \'int\'>'},
            'transform': {
                'jq_filter': 'Wrong type: found <type \'int\'>, expected <type \'basestring\'>'},
            'persistence': {
                'file_path': 'Wrong type: found <type \'int\'>, expected <type \'basestring\'>'}
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
                'name': 'ozzy.writers.console_writer.ConsoleWriter',
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

    def test_valid_formatter(self):
        options = valid_config_with_updates({
            'exporter_options': {
                "formatter": {
                    "name": "ozzy.export_formatter.json_export_formatter.JsonExportFormatter"
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
                'name': 'ozzy.filters.key_value_filters.KeyValueFilter',
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
                    "name": "ozzy.notifications.ses_mail_notifier.SESMailNotifier",
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
                    "name": "ozzy.export_formatter.json_export_formatter.JsonExportFormatter",
                    "options": {
                        "unsuported_option": True
                    }
                }]
            }
        })
        with self.assertRaisesRegexp(ValueError, 'unsupported_options'):
            ExporterConfig(options)
