import unittest
from exporters.export_managers.bypass import BaseBypass, S3Bypass, RequisitesNotMet
from exporters.groupers.base_grouper import BaseGrouper
from exporters.logger.base_logger import CategoryLogger
from exporters.pipeline.base_pipeline_item import BasePipelineItem
from exporters.config_api import ConfigApi, InvalidConfigError, \
    IncompatibleModulesUsedError
from exporters.exceptions import InvalidExpression
from exporters.module_loader import ModuleLoader
from exporters.exporter_config import ExporterConfig
from exporters.python_interpreter import Interpreter


class BaseLoggerTest(unittest.TestCase):
    def setUp(self):
        self.options = {
            'exporter_options': {
                'log_level': 'DEBUG',
            },
            'reader': {},
            'writer': {}
        }

    def test_category_warning(self):
        options = ExporterConfig(self.options)
        logger = CategoryLogger(options.log_options)
        logger.warning('Warning message')

    def test_category_critical(self):
        options = ExporterConfig(self.options)
        logger = CategoryLogger(options.log_options)
        logger.critical('Critial message')


class BasePipelineItemTest(unittest.TestCase):

    def test_false_required(self):
        pipelineItem = BasePipelineItem({})
        pipelineItem.supported_options = {'number_of_items': {'type': int, 'default': 10}}
        pipelineItem.check_options()

    def test_not_present(self):
        pipelineItem = BasePipelineItem({})
        pipelineItem.supported_options = {'number_of_items': {'type': int}}
        with self.assertRaises(ValueError):
            pipelineItem.check_options()

    def test_wrong_type(self):
        pipelineItem = BasePipelineItem({'options': {'number_of_items': 'wrong_string'}})
        pipelineItem.supported_options = {'number_of_items': {'type': int, 'default': 10}}
        with self.assertRaises(ValueError):
            pipelineItem.check_options()


class ConfigApiTest(unittest.TestCase):
    def setUp(self):
        self.config_api = ConfigApi()

    def test_get_supported_options(self):
        for reader in self.config_api.readers:
            supported_options = self.config_api.get_module_supported_options(reader)
            print supported_options
            for requirement_name, requirement_info in supported_options.iteritems():
                self.assertIsInstance(requirement_info, dict)
                self.assertIsInstance(requirement_name, basestring)

    def test_get_modules_by_type(self):
        self.assertIsInstance(self.config_api.writers, list)
        self.assertIsInstance(self.config_api.readers, list)
        self.assertIsInstance(self.config_api.persistence, list)
        self.assertIsInstance(self.config_api.filters, list)
        self.assertIsInstance(self.config_api.transforms, list)
        self.assertIsInstance(self.config_api.groupers, list)

    def test_get_wrong_module_name(self):
        with self.assertRaises(InvalidConfigError):
            self.config_api.get_module_supported_options('not a valid module name')

    def test_find_missing_sections(self):
        with self.assertRaises(InvalidConfigError):
            self.config_api.check_valid_config({})

    def test_check_configuration(self):
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
            'filter_before': {
                'name': 'exporters.filters.no_filter.NoFilter',
                'options': {}
            },
            'filter_after': {
                'name': 'exporters.filters.no_filter.NoFilter',
                'options': {}
            },
            'transform': {
                'name': 'exporters.transform.no_transform.NoTransform',
                'options': {}
            },
            'exporter_options': {},
            'persistence': {
                'name': 'exporters.persistence.pickle_persistence.PicklePersistence',
                'options': {
                    'file_base': '/tmp'
                }
            },
            'grouper': {
                'name': 'exporters.grouper.no_grouper.NoGrouper',
                'options': {

                }
            }
        }
        self.assertIs(self.config_api.check_valid_config(config), True)

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
                    'file_base': '/tmp'
                }
            }
        }
        with self.assertRaises(InvalidConfigError):
            self.config_api.check_valid_config(config)

    def test_wrong_type_supported_options(self):
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
                'options': {
                    'jq_filter': 5
                }
            },
            'exporter_options': {},
            'persistence': {
                'name': 'exporters.persistence.PicklePersistence',
                'options': {
                    'file_base': '/tmp'
                }
            }
        }
        with self.assertRaises(InvalidConfigError):
            self.config_api.check_valid_config(config)

    def test_missing_items_in_config_section(self):
        with self.assertRaises(InvalidConfigError):
            self.config_api._check_valid_options({})

    def test_check_valid_grouper(self):
        grouper = {
            'name': 'exporters.groupers.no_grouper.NoGrouper',
            'options': {}
        }

        self.assertIs(self.config_api._check_valid_options(grouper), None)

    def test_incompatible_configuration(self):
        config = {
            'reader': {
                'name': 'exporters.readers.random_reader.RandomReader',
                'options': {}
            },
            'writer': {
                'name': 'exporters.writers.console_writer.ConsoleWriter',
                'options': {}
            },
            'exporter_options': {
                "formatter": {
                    "name": "exporters.export_formatter.csv_export_formatter.CSVExportFormatter",
                    "options": {

                    }
                }
            },

        }
        with self.assertRaises(IncompatibleModulesUsedError):
            self.config_api.check_compatibility(config)

    def test_incompatible_ignore_configuration(self):
        config = {
            'reader': {
                'name': 'exporters.readers.random_reader.RandomReader',
                'options': {}
            },
            'writer': {
                'name': 'exporters.writers.mail_writer.MailWriter',
                'options': {}
            },
            'exporter_options': {
                "notifications": [
                    {
                        'name': 'exporters.notifications.s3_mail_notifier.S3MailNotifier',
                        'options': {}
                    }
                ]
            },

        }
        new_config = self.config_api.check_compatibility(config)
        self.assertEquals(len(new_config['exporter_options']['notifications']), 0)


class ModuleLoaderTest(unittest.TestCase):
    def setUp(self):
        self.module_loader = ModuleLoader()

    def test_reader_valid_class(self):
        options = {
            'exporter_options': {
                'LOG_LEVEL': 'DEBUG',
                'LOGGER_NAME': 'export-pipeline'
            },
            'reader': {
                'name': 'exporters.transform.no_transform.NoTransform',
                'options': {
                    'number_of_items': 1000,
                    'batch_size': 100
                }
            },
            'writer':{}
        }
        with self.assertRaises(TypeError):
            o = ExporterConfig(options)
            self.module_loader.load_reader(o.reader_options)

    def test_writer_valid_class(self):
        options = {
            'exporter_options': {
                'LOG_LEVEL': 'DEBUG',
                'LOGGER_NAME': 'export-pipeline'
            },
            'reader': {},
            'writer': {
                'name': 'exporters.readers.random_reader.RandomReader',
                'options': {
                    'number_of_items': 1000,
                    'batch_size': 100
                }
            },
        }
        with self.assertRaises(TypeError):
            o = ExporterConfig(options)
            self.module_loader.load_writer(o.writer_options)

    def test_persistence_valid_class(self):
        options = {
            'exporter_options': {
                'LOG_LEVEL': 'DEBUG',
                'LOGGER_NAME': 'export-pipeline'
            },
            'reader': {},
            'writer': {},
            'persistence': {
                'name': 'exporters.transform.no_transform.NoTransform',
                'options': {
                    'number_of_items': 1000,
                    'batch_size': 100
                }
            },
        }
        o = ExporterConfig(options)
        with self.assertRaises(TypeError):
            self.module_loader.load_persistence(o.persistence_options)

    def test_formatter_valid_class(self):
        options = {
            'exporter_options': {
                'LOG_LEVEL': 'DEBUG',
                'LOGGER_NAME': 'export-pipeline',
                "EXPORTER": 'exporters.writers.console_writer.ConsoleWriter',
            },
            'reader': {
                'name': 'exporters.transform.no_transform.NoTransform',
                'options': {
                    'number_of_items': 1000,
                    'batch_size': 100
                }
            },
            'writer': {}
        }
        with self.assertRaises(TypeError):
            o = ExporterConfig(options)
            self.module_loader.load_formatter(o.reader_options)

    def test_notifier_valid_class(self):
        options = {
            'exporter_options': {
                'LOG_LEVEL': 'DEBUG',
                'LOGGER_NAME': 'export-pipeline'
            },
            'notifier': {
                'name': 'exporters.transform.no_transform.NoTransform',
                'options': {
                    'number_of_items': 1000,
                    'batch_size': 100
                }
            },
            'reader': {},
            'writer': {}
        }
        with self.assertRaises(TypeError):
            o = ExporterConfig(options)
            self.module_loader.load_notifier(o.notifiers)

    def test_grouper_valid_class(self):
        options = {
            'exporter_options': {
                'LOG_LEVEL': 'DEBUG',
                'LOGGER_NAME': 'export-pipeline'
            },
            'grouper': {
                'name': 'exporters.transform.no_transform.NoTransform',
                'options': {
                    'number_of_items': 1000,
                    'batch_size': 100
                }
            },
            'reader':{},
            'writer': {}
        }
        with self.assertRaises(TypeError):
            o = ExporterConfig(options)
            self.module_loader.load_grouper(o.grouper_options)

    def test_grouper_valid_class(self):
        options = {
            'exporter_options': {
                'LOG_LEVEL': 'DEBUG',
                'LOGGER_NAME': 'export-pipeline'
            },
            'grouper': {
                'name': 'exporters.transform.no_transform.NoTransform',
                'options': {
                    'number_of_items': 1000,
                    'batch_size': 100
                }
            },
            'reader': {},
            'writer': {}
        }
        with self.assertRaises(TypeError):
            o = ExporterConfig(options)
            self.module_loader.load_grouper(o.grouper_options)

    def test_filter_valid_class(self):
        options = {
            'exporter_options': {
                'LOG_LEVEL': 'DEBUG',
                'LOGGER_NAME': 'export-pipeline'
            },
            'reader': {},
            'writer': {},
            'filter': {
                'name': 'exporters.transform.no_transform.NoTransform',
                'options': {
                    'number_of_items': 1000,
                    'batch_size': 100
                }
            },
        }
        with self.assertRaises(TypeError):
            o = ExporterConfig(options)
            self.module_loader.load_filter(o.filter_before_options)

    def test_transform_valid_class(self):
        options = {
            'exporter_options': {
                'LOG_LEVEL': 'DEBUG',
                'LOGGER_NAME': 'export-pipeline'
            },
            'reader': {},
            'writer': {},
            'transform': {
                'name': 'exporters.filters.no_filter.NoFilter',
                'options': {
                    'number_of_items': 1000,
                    'batch_size': 100
                }
            },
        }
        with self.assertRaises(TypeError):
            o = ExporterConfig(options)
            self.module_loader.load_transform(o.transform_options)

    def test_load_grouper(self):
        options = {
            'exporter_options': {
                'LOG_LEVEL': 'DEBUG',
                'LOGGER_NAME': 'export-pipeline'
            },
            'grouper': {
                'name': 'exporters.groupers.file_key_grouper.FileKeyGrouper',
                'options': {
                    'keys': ['country_code', 'state', 'city']
                }
            },
        }
        self.assertIsInstance(self.module_loader.load_grouper(options['grouper']), BaseGrouper)


class OptionsParserTest(unittest.TestCase):
    def test_curate_options(self):
        options = {}
        with self.assertRaises(Exception):
            ExporterConfig(options)
        options = {'reader': ''}
        with self.assertRaises(Exception):
            ExporterConfig(options)
        options = {'reader': '', 'filter': ''}
        with self.assertRaises(Exception):
            ExporterConfig(options)
        options = {'reader': '', 'filter': '', 'transform': ''}
        with self.assertRaises(Exception):
            ExporterConfig(options)
        options = {'reader': '', 'filter': '', 'transform': '', 'writer': ''}
        with self.assertRaises(Exception):
            ExporterConfig(options)
        options = {'reader': {}, 'filter': {}, 'transform': {}, 'writer': {}, 'persistence': {},
                   'exporter_options': {'formatter': {}}}
        self.assertIsInstance(ExporterConfig(options), ExporterConfig)


class PythonInterpreterTest(unittest.TestCase):
    def setUp(self):
        self.interpreter = Interpreter()

    def test_check(self):
        with self.assertRaises(InvalidExpression):
            self.interpreter.check(5)
        with self.assertRaises(InvalidExpression):
            self.interpreter.check('')
        with self.assertRaises(SyntaxError):
            self.interpreter.check('This is not a valid expression')
        with self.assertRaises(InvalidExpression):
            self.interpreter.check('2+2; 5+6')


class BaseByPassTest(unittest.TestCase):
    def test_not_implemented(self):
        bypass_script = BaseBypass({})
        with self.assertRaises(NotImplementedError):
            bypass_script.meets_conditions()
        with self.assertRaises(NotImplementedError):
            bypass_script.bypass()


class S3ByPassTest(unittest.TestCase):

    def test_not_meet_supported_options(self):
        exporter_options = ExporterConfig({
            'reader': {'name': 'some other reader'},
            'writer': {'name': 'exporters.writers.s3_writer.S3Writer'},
            'exporter_options': {'formatter': {}},
            'persistence': {}
        })
        bypass = S3Bypass(exporter_options)
        with self.assertRaises(RequisitesNotMet):
            bypass.meets_conditions()

    def test_meet_supported_options(self):
        exporter_options = ExporterConfig({
            'reader': {'name': 'exporters.readers.s3_reader.S3Reader'},
            'writer': {'name': 'exporters.writers.s3_writer.S3Writer'},
            'exporter_options': {'formatter': {}},
            'persistence': {}
        })
        bypass_script = S3Bypass(exporter_options)
        bypass_script.meets_conditions()
