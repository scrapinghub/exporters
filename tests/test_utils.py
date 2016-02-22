import os
import unittest
from decorator import contextmanager
from exporters.exporter_config import (check_for_errors, module_options,
                                       MODULE_TYPES)
from exporters.export_managers.s3_to_s3_bypass import BaseBypass, S3Bypass
from exporters.export_managers.base_bypass import RequisitesNotMet, BaseBypass
from exporters.groupers.base_grouper import BaseGrouper
from exporters.logger.base_logger import CategoryLogger
from exporters.pipeline.base_pipeline_item import BasePipelineItem
from exporters.exceptions import (InvalidExpression, ConfigurationError,
                                  ConfigCheckError)
from exporters.module_loader import ModuleLoader
from exporters.exporter_config import ExporterConfig
from exporters.python_interpreter import Interpreter

from .utils import VALID_EXPORTER_CONFIG, valid_config_with_updates


class BaseLoggerTest(unittest.TestCase):
    def setUp(self):
        self.options = valid_config_with_updates({
            'exporter_options': {'log_level': 'DEBUG'}
        })

    def test_category_warning(self):
        options = ExporterConfig(self.options)
        logger = CategoryLogger(options.log_options)
        logger.warning('Warning message')

    def test_category_critical(self):
        options = ExporterConfig(self.options)
        logger = CategoryLogger(options.log_options)
        logger.critical('Critial message')


@contextmanager
def environment(env):
    original_env = os.environ
    os.environ = env
    try:
        yield
    finally:
        os.environ = original_env


class BasePipelineItemTest(unittest.TestCase):
    def test_pipeline_item_with_type_declared(self):
        class MyPipelineItem(BasePipelineItem):
            supported_options = {'opt1': {'type': int}}

        with self.assertRaisesRegexp(ValueError, 'Value for option .* should be of type'):
            MyPipelineItem({'options': {'opt1': 'string'}})

    def test_pipeline_item_with_env_fallback(self):
        class MyPipelineItem(BasePipelineItem):
            supported_options = {'opt1': {'type': basestring, 'env_fallback': 'ENV_TEST'}}

        with environment({'ENV_TEST': 'test'}):
            instance = MyPipelineItem({})
            self.assertIs(instance.read_option('opt1'), 'test')

        with self.assertRaisesRegexp(ConfigurationError, "Missing value for option"):
            MyPipelineItem({})

    def test_pipeline_item_with_env_fallback_and_default(self):
        class MyPipelineItem(BasePipelineItem):
            supported_options = {
                'opt1': {
                    'type': basestring,
                    'default': 'default_value',
                    'env_fallback': 'ENV_TEST'
                },
            }

        instance = MyPipelineItem({})
        self.assertIs(instance.read_option('opt1'), 'default_value')

        with environment({'ENV_TEST': 'test'}):
            instance = MyPipelineItem({})
            self.assertIs(instance.read_option('opt1'), 'test')

        with environment({'ENV_TEST': 'test'}):
            instance = MyPipelineItem({'options': {'opt1': 'given_value'}})
            self.assertIs(instance.read_option('opt1'), 'given_value')

        with environment({'ENV_TEST': ''}):
            instance = MyPipelineItem({})
            self.assertIs(instance.read_option('opt1'), '')

    def test_pipeline_item_with_no_env_fallback_and_default_and_value(self):
        class MyPipelineItem(BasePipelineItem):
            supported_options = {'opt1': {'type': basestring, 'default': 'default_value'}}

        instance = MyPipelineItem({'options': {'opt1': 'given_value'}})

        self.assertIs(instance.read_option('opt1'), 'given_value')

    def test_simple_supported_option(self):
        class MyPipelineItem(BasePipelineItem):
            supported_options = {'opt1': {'type': basestring}}

        instance = MyPipelineItem({'options': {'opt1': 'given_value'}})
        self.assertIs(instance.read_option('opt1'), 'given_value')

        with self.assertRaisesRegexp(ValueError, "Missing value for option"):
            MyPipelineItem({'options': {}})


class ConfigModuleOptionsTest(unittest.TestCase):
    def test_module_options(self):
        options = module_options()
        self.assertItemsEqual(MODULE_TYPES, options.keys())
        for modules in options.values():
            self.assertIsInstance(modules, list)


class ConfigCheckErrorTest(unittest.TestCase):
    def cce_to_str(self, *args, **kwargs):
        return str(ConfigCheckError(*args, **kwargs))

    def test_just_message(self):
        self.assertEqual(self.cce_to_str(message='str message'),
                         'str message')

    def test_section_missing(self):
        self.assertEqual(self.cce_to_str(message='msg', errors={'sec': 'missing'}),
                         'msg\nsec: missing')

    def test_many_errors(self):
        errors = {
            'sec1': 'missing',
            'sec2': {'number_of_items': 'bad',
                     'batch_size': 'very bad'},
            'sec3': {'single_field': 'invalid'}
        }
        def sorted_lines(s):
            return sorted(s.split())
        # dicts are not ordered, so we use not quite exact comparison
        self.assertEqual(sorted_lines(self.cce_to_str(message='msg', errors=errors)),
                         sorted_lines("""msg
sec1: missing
sec2:
  number_of_items: bad
  batch_size: very bad
sec3:
  single_field: invalid"""))


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


class ModuleLoaderTest(unittest.TestCase):
    def setUp(self):
        self.module_loader = ModuleLoader()

    def test_reader_valid_class(self):
        options = valid_config_with_updates({
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
            }
        })
        with self.assertRaises(TypeError):
            o = ExporterConfig(options)
            self.module_loader.load_reader(o.reader_options)

    def test_writer_valid_class(self):
        options = valid_config_with_updates({
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
        })
        with self.assertRaises(TypeError):
            o = ExporterConfig(options)
            self.module_loader.load_writer(o.writer_options)

    def test_persistence_valid_class(self):
        options = valid_config_with_updates({
            'exporter_options': {
                'LOG_LEVEL': 'DEBUG',
                'LOGGER_NAME': 'export-pipeline'
            },
            'persistence': {
                'name': 'exporters.transform.no_transform.NoTransform',
                'options': {
                    'number_of_items': 1000,
                    'batch_size': 100
                }
            }
        })
        o = ExporterConfig(options)
        with self.assertRaises(TypeError):
            self.module_loader.load_persistence(o.persistence_options)

    def test_formatter_valid_class(self):
        options = valid_config_with_updates({
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
        })
        with self.assertRaises(TypeError):
            o = ExporterConfig(options)
            self.module_loader.load_formatter(o.reader_options)

    def test_notifier_valid_class(self):
        options = valid_config_with_updates({
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
            }
        })
        with self.assertRaises(TypeError):
            o = ExporterConfig(options)
            self.module_loader.load_notifier(o.notifiers)

    def test_grouper_valid_class(self):
        options = valid_config_with_updates({
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
            }
        })
        with self.assertRaises(TypeError):
            o = ExporterConfig(options)
            self.module_loader.load_grouper(o.grouper_options)

    def test_filter_valid_class(self):
        options = valid_config_with_updates({
            'exporter_options': {
                'LOG_LEVEL': 'DEBUG',
                'LOGGER_NAME': 'export-pipeline'
            },
            'filter': {
                'name': 'exporters.transform.no_transform.NoTransform',
                'options': {
                    'number_of_items': 1000,
                    'batch_size': 100
                }
            },
        })
        with self.assertRaises(TypeError):
            o = ExporterConfig(options)
            self.module_loader.load_filter(o.filter_before_options)

    def test_transform_valid_class(self):
        options = valid_config_with_updates({
            'exporter_options': {
                'LOG_LEVEL': 'DEBUG',
                'LOGGER_NAME': 'export-pipeline'
            },
            'transform': {
                'name': 'exporters.filters.no_filter.NoFilter',
                'options': {
                    'number_of_items': 1000,
                    'batch_size': 100
                }
            }
        })
        with self.assertRaises(TypeError):
            o = ExporterConfig(options)
            self.module_loader.load_transform(o.transform_options)

    def test_load_grouper(self):
        grouper = {
            'name': 'exporters.groupers.file_key_grouper.FileKeyGrouper',
            'options': {
                    'keys': ['country_code', 'state', 'city']
            }
        }
        self.assertIsInstance(self.module_loader.load_grouper(grouper),
                              BaseGrouper)


class OptionsParserTest(unittest.TestCase):
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
        exporter_options = ExporterConfig(
            valid_config_with_updates({
                'writer': {'name': 'exporters.writers.s3_writer.S3Writer',
                           'options': {'bucket': 'mock', 'filebase': 'mock'}},
                'exporter_options': {'formatter': {}}
            })
        )
        bypass = S3Bypass(exporter_options)
        with self.assertRaises(RequisitesNotMet):
            bypass.meets_conditions()

    def test_meet_supported_options(self):
        exporter_options = ExporterConfig(
            valid_config_with_updates({
                'reader': {'name': 'exporters.readers.s3_reader.S3Reader',
                           'options': {'prefix': 'mock', 'bucket': 'mock'}},
                'writer': {'name': 'exporters.writers.s3_writer.S3Writer',
                           'options': {'bucket': 'mock', 'filebase': 'mock'}},
                'exporter_options': {'formatter': {}}
            })
        )
        bypass_script = S3Bypass(exporter_options)
        bypass_script.meets_conditions()
