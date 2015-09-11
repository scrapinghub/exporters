import unittest
from exporters.export_managers.bypass import BaseBypass, S3Bypass, RequisitesNotMet
from exporters.export_managers.settings import Settings
from exporters.groupers.base_grouper import BaseGrouper
from exporters.logger.base_logger import CategoryLogger
from exporters.pipeline.base_pipeline_item import BasePipelineItem
from exporters.config_api import ConfigApi, InvalidConfigError
from exporters.exceptions import InvalidExpression
from exporters.module_loader import ModuleLoader
from exporters.exporter_options import ExporterOptions
from exporters.python_interpreter import Interpreter


class SettingsTest(unittest.TestCase):
    retry_counter = 0

    def setUp(self):
        self.options = {
            'exporter_options': {
                'NUMBER_OF_RETRIES': 6,
            }
        }

    def test_single_settings(self):
        settings = Settings(self.options['exporter_options'])
        self.assertIsInstance(settings, Settings)

    def test_get_none(self):
        settings = Settings(self.options['exporter_options'])
        value = settings.get('some_value')
        self.assertTrue(value == None)
        self.assertIsInstance(settings, Settings)


class BaseLoggerTest(unittest.TestCase):
    def setUp(self):
        self.options = {
            'exporter_options': {
                'log_level': 'DEBUG',
            }
        }
        self.settings = Settings(self.options['exporter_options'])

    def test_category_warning(self):
        logger = CategoryLogger(self.settings)
        logger.warning('Warning message')

    def test_category_critical(self):
        logger = CategoryLogger(self.settings)
        logger.critical('Critial message')


class BasePipelineItemTest(unittest.TestCase):
    def setUp(self):
        self.options = {
            'exporter_options': {
            },
        }
        self.settings = Settings(self.options['exporter_options'])

    def test_false_required(self):
        pipelineItem = BasePipelineItem({}, self.settings)
        pipelineItem.parameters = {'number_of_items': {'type': int, 'required': False, 'default': 10}}
        pipelineItem.check_options()

    def test_not_present(self):
        pipelineItem = BasePipelineItem({}, self.settings)
        pipelineItem.parameters = {'number_of_items': {'type': int, 'required': True}}
        with self.assertRaises(ValueError):
            pipelineItem.check_options()

    def test_wrong_type(self):
        pipelineItem = BasePipelineItem({'options': {'number_of_items': 'wrong_string'}}, self.settings)
        pipelineItem.parameters = {'number_of_items': {'type': int, 'required': False, 'default': 10}}
        with self.assertRaises(ValueError):
            pipelineItem.check_options()


class ConfigApiTest(unittest.TestCase):
    def setUp(self):
        self.config_api = ConfigApi()

    def test_get_parameters(self):
        for reader in self.config_api.readers:
            parameters = self.config_api.get_module_parameters(reader)
            print parameters
            for requirement_name, requirement_info in parameters.iteritems():
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
            self.config_api.get_module_parameters('not a valid module name')

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

    def test_missing_parameters(self):
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

    def test_wrong_type_parameters(self):
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
            self.config_api._check_valid_parameters({})

    def test_check_valid_grouper(self):
        grouper = {
            'name': 'exporters.groupers.no_grouper.NoGrouper',
            'options': {}
        }

        self.assertIs(self.config_api._check_valid_parameters(grouper), None)


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
                'name': 'exporters.writers.console_writer.ConsoleWriter',
                'options': {
                    'number_of_items': 1000,
                    'batch_size': 100
                }
            },
        }
        settings = Settings(options['exporter_options'])
        with self.assertRaises(Exception):
            self.module_loader.load_reader(options['reader'], settings)

    def test_writer_valid_class(self):
        options = {
            'exporter_options': {
                'LOG_LEVEL': 'DEBUG',
                'LOGGER_NAME': 'export-pipeline'
            },
            'writer': {
                'name': 'exporters.readers.random_reader.RandomReader',
                'options': {
                    'number_of_items': 1000,
                    'batch_size': 100
                }
            },
        }
        settings = Settings(options['exporter_options'])
        with self.assertRaises(Exception):
            self.module_loader.load_writer(options['writer'], settings)

    def test_persistence_valid_class(self):
        options = {
            'exporter_options': {
                'LOG_LEVEL': 'DEBUG',
                'LOGGER_NAME': 'export-pipeline'
            },
            'persistence': {
                'name': 'exporters.writers.console_writer.ConsoleWriter',
                'options': {
                    'number_of_items': 1000,
                    'batch_size': 100
                }
            },
        }
        settings = Settings(options['exporter_options'])
        with self.assertRaises(Exception):
            self.module_loader.load_persistence(options['persistence'], settings)

    def test_formatter_valid_class(self):
        options = {
            'exporter_options': {
                'LOG_LEVEL': 'DEBUG',
                'LOGGER_NAME': 'export-pipeline',
                "EXPORTER": 'exporters.writers.console_writer.ConsoleWriter',
            },
            'reader': {
                'name': 'exporters.writers.console_writer.ConsoleWriter',
                'options': {
                    'number_of_items': 1000,
                    'batch_size': 100
                }
            },
        }
        settings = Settings(options['exporter_options'])
        with self.assertRaises(Exception):
            self.module_loader.load_formatter(options['reader'], settings)

    def test_notifier_valid_class(self):
        options = {
            'exporter_options': {
                'LOG_LEVEL': 'DEBUG',
                'LOGGER_NAME': 'export-pipeline'
            },
            'notifier': {
                'name': 'exporters.writers.console_writer.ConsoleWriter',
                'options': {
                    'number_of_items': 1000,
                    'batch_size': 100
                }
            },
        }
        settings = Settings(options['exporter_options'])
        with self.assertRaises(Exception):
            self.module_loader.load_notifier(options['notifier'], settings)

    def test_grouper_valid_class(self):
        options = {
            'exporter_options': {
                'LOG_LEVEL': 'DEBUG',
                'LOGGER_NAME': 'export-pipeline'
            },
            'grouper': {
                'name': 'exporters.writers.console_writer.ConsoleWriter',
                'options': {
                    'number_of_items': 1000,
                    'batch_size': 100
                }
            },
        }
        settings = Settings(options['exporter_options'])
        with self.assertRaises(Exception):
            self.module_loader.load_grouper(options['grouper'], settings)

    def test_grouper_valid_class(self):
        options = {
            'exporter_options': {
                'LOG_LEVEL': 'DEBUG',
                'LOGGER_NAME': 'export-pipeline'
            },
            'grouper': {
                'name': 'exporters.writers.console_writer.ConsoleWriter',
                'options': {
                    'number_of_items': 1000,
                    'batch_size': 100
                }
            },
        }
        settings = Settings(options['exporter_options'])
        with self.assertRaises(Exception):
            self.module_loader.load_grouper(options['grouper'], settings)

    def test_filter_valid_class(self):
        options = {
            'exporter_options': {
                'LOG_LEVEL': 'DEBUG',
                'LOGGER_NAME': 'export-pipeline'
            },
            'filter': {
                'name': 'exporters.writers.console_writer.ConsoleWriter',
                'options': {
                    'number_of_items': 1000,
                    'batch_size': 100
                }
            },
        }
        settings = Settings(options['exporter_options'])
        with self.assertRaises(Exception):
            self.module_loader.load_filter(options['filter'], settings)

    def test_transform_valid_class(self):
        options = {
            'exporter_options': {
                'LOG_LEVEL': 'DEBUG',
                'LOGGER_NAME': 'export-pipeline'
            },
            'transform': {
                'name': 'exporters.writers.console_writer.ConsoleWriter',
                'options': {
                    'number_of_items': 1000,
                    'batch_size': 100
                }
            },
        }
        settings = Settings(options['exporter_options'])
        with self.assertRaises(Exception):
            self.module_loader.load_transform(options['transform'], settings)

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
        settings = Settings(options['exporter_options'])
        self.assertIsInstance(self.module_loader.load_grouper(options['grouper'], settings), BaseGrouper)


class OptionsParserTest(unittest.TestCase):
    def test_curate_options(self):
        options = {}
        with self.assertRaises(Exception):
            ExporterOptions(options)
        options = {'reader': ''}
        with self.assertRaises(Exception):
            ExporterOptions(options)
        options = {'reader': '', 'filter': ''}
        with self.assertRaises(Exception):
            ExporterOptions(options)
        options = {'reader': '', 'filter': '', 'transform': ''}
        with self.assertRaises(Exception):
            ExporterOptions(options)
        options = {'reader': '', 'filter': '', 'transform': '', 'writer': ''}
        with self.assertRaises(Exception):
            ExporterOptions(options)
        options = {'reader': '', 'filter': '', 'transform': '', 'writer': '', 'persistence': '',
                   'exporter_options': {'formatter': {}}}
        self.assertIsInstance(ExporterOptions(options), ExporterOptions)


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

    def test_not_meet_parameters(self):
        exporter_options = ExporterOptions({
            'reader': {'name': 'some other reader'},
            'writer': {'name': 'exporters.writers.s3_writer.S3Writer'},
            'exporter_options': {'formatter': {}},
            'persistence': {}
        })
        bypass = S3Bypass(exporter_options)
        with self.assertRaises(RequisitesNotMet):
            bypass.meets_conditions()

    def test_meet_parameters(self):
        exporter_options = ExporterOptions({
            'reader': {'name': 'exporters.readers.s3_reader.S3Reader'},
            'writer': {'name': 'exporters.writers.s3_writer.S3Writer'},
            'exporter_options': {'formatter': {}},
            'persistence': {}
        })
        bypass_script = S3Bypass(exporter_options)
        bypass_script.meets_conditions()
