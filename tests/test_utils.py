import unittest

from ozzy.bypasses.base import BaseBypass
from ozzy.bypasses.s3_to_s3_bypass import S3Bypass
from ozzy.exceptions import (InvalidExpression, ConfigurationError,
                             ConfigCheckError)
from ozzy.exporter_config import ExporterConfig
from ozzy.exporter_config import (module_options,
                                  MODULE_TYPES)
from ozzy.groupers.base_grouper import BaseGrouper
from ozzy.logger.base_logger import CategoryLogger
from ozzy.module_loader import ModuleLoader
from ozzy.pipeline.base_pipeline_item import BasePipelineItem
from ozzy.python_interpreter import Interpreter
from ozzy.utils import nested_dict_value, TmpFile, split_file, \
    calculate_multipart_etag, str_list, dict_list, int_list, maybe_cast_list
from .utils import environment
from .utils import valid_config_with_updates


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


class BasePipelineItemTest(unittest.TestCase):
    def test_pipeline_item_with_type_declared(self):
        class MyPipelineItem(BasePipelineItem):
            supported_options = {'opt1': {'type': int}}

        with self.assertRaisesRegexp(ValueError, 'Value for option .* should be of type'):
            MyPipelineItem({'options': {'opt1': 'string'}}, None)

    def test_pipeline_item_with_env_fallback(self):
        class MyPipelineItem(BasePipelineItem):
            supported_options = {'opt1': {'type': basestring, 'env_fallback': 'ENV_TEST'}}

        with environment({'ENV_TEST': 'test'}):
            instance = MyPipelineItem({}, None)
            self.assertIs(instance.read_option('opt1'), 'test')

        with self.assertRaisesRegexp(ConfigurationError, "Missing value for option"):
            MyPipelineItem({}, None)

    def test_pipeline_item_with_env_fallback_and_default(self):
        class MyPipelineItem(BasePipelineItem):
            supported_options = {
                'opt1': {
                    'type': basestring,
                    'default': 'default_value',
                    'env_fallback': 'ENV_TEST'
                },
            }

        instance = MyPipelineItem({}, None)
        self.assertIs(instance.read_option('opt1'), 'default_value')

        with environment({'ENV_TEST': 'test'}):
            instance = MyPipelineItem({}, None)
            self.assertIs(instance.read_option('opt1'), 'test')

        with environment({'ENV_TEST': 'test'}):
            instance = MyPipelineItem({'options': {'opt1': 'given_value'}}, None)
            self.assertIs(instance.read_option('opt1'), 'given_value')

        with environment({'ENV_TEST': ''}):
            instance = MyPipelineItem({}, None)
            self.assertIs(instance.read_option('opt1'), '')

    def test_pipeline_item_with_no_env_fallback_and_default_and_value(self):
        class MyPipelineItem(BasePipelineItem):
            supported_options = {'opt1': {'type': basestring, 'default': 'default_value'}}

        instance = MyPipelineItem({'options': {'opt1': 'given_value'}}, None)

        self.assertIs(instance.read_option('opt1'), 'given_value')

    def test_simple_supported_option(self):
        class MyPipelineItem(BasePipelineItem):
            supported_options = {'opt1': {'type': basestring}}

        instance = MyPipelineItem({'options': {'opt1': 'given_value'}}, None)
        self.assertIs(instance.read_option('opt1'), 'given_value')

        with self.assertRaisesRegexp(ValueError, "Missing value for option"):
            MyPipelineItem({'options': {}}, None)


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
                'name': 'ozzy.transform.no_transform.NoTransform',
                'options': {
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
                'name': 'ozzy.readers.random_reader.RandomReader',
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
                'name': 'ozzy.transform.no_transform.NoTransform',
                'options': {
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
                "EXPORTER": 'ozzy.writers.console_writer.ConsoleWriter',
            },
            'formatter': {
                'name': 'ozzy.transform.no_transform.NoTransform',
                'options': {
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
                'name': 'ozzy.transform.no_transform.NoTransform',
                'options': {
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
                'name': 'ozzy.transform.no_transform.NoTransform',
                'options': {
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
                'name': 'ozzy.transform.no_transform.NoTransform',
                'options': {
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
                'name': 'ozzy.filters.no_filter.NoFilter',
                'options': {
                }
            }
        })
        with self.assertRaises(TypeError):
            o = ExporterConfig(options)
            self.module_loader.load_transform(o.transform_options)

    def test_load_grouper(self):
        grouper = {
            'name': 'ozzy.groupers.file_key_grouper.FileKeyGrouper',
            'options': {
                    'keys': ['country_code', 'state', 'city']
            }
        }
        self.assertIsInstance(self.module_loader.load_grouper(grouper, None),
                              BaseGrouper)


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
        bypass_script = BaseBypass(ExporterConfig(valid_config_with_updates({})), None)
        with self.assertRaises(NotImplementedError):
            BaseBypass.meets_conditions({})
        with self.assertRaises(NotImplementedError):
            bypass_script.execute()

JSON_FORMATTER = {"name": "ozzy.export_formatter.json_export_formatter.JsonExportFormatter"}


class S3ByPassTest(unittest.TestCase):
    def test_not_meet_supported_options(self):
        exporter_options = ExporterConfig(
            valid_config_with_updates({
                'writer': {'name': 'ozzy.writers.s3_writer.S3Writer',
                           'options': {'bucket': 'mock', 'filebase': 'mock'}},
                'exporter_options': {'formatter': JSON_FORMATTER}
            })
        )
        self.assertFalse(S3Bypass.meets_conditions(exporter_options))

    def test_meet_supported_options(self):
        exporter_options = ExporterConfig(
            valid_config_with_updates({
                'reader': {'name': 'ozzy.readers.s3_reader.S3Reader',
                           'options': {'prefix': 'mock', 'bucket': 'mock'}},
                'writer': {'name': 'ozzy.writers.s3_writer.S3Writer',
                           'options': {'bucket': 'mock', 'filebase': 'mock'}},
                'exporter_options': {'formatter': JSON_FORMATTER}
            })
        )
        S3Bypass.meets_conditions(exporter_options)


class NesteDictReadTest(unittest.TestCase):

    def get_nested_dict(self):
        nested_dict = {
            'address': {
                'street': {
                    'name': 'some_name',
                    'number': 123
                }
            },
            'city': 'val',
            'country': 'us'
        }
        return nested_dict

    def test_get_the_right_value(self):
        # given
        nested_dict = self.get_nested_dict()

        # when
        number = nested_dict_value(nested_dict, 'address.street.number'.split('.'))
        city = nested_dict_value(nested_dict, ['city'])

        # then
        self.assertEqual(number, 123)
        self.assertEqual(city, 'val')

    def test_get_not_a_value(self):
        # given
        nested_dict = self.get_nested_dict()

        # when
        with self.assertRaisesRegexp(KeyError, 'could not be found for nested path'):
            nested_dict_value(nested_dict, 'address.postal_code'.split('.'))

    def test_get_none_value(self):
        with self.assertRaisesRegexp(TypeError, 'Could not get key'):
            nested_dict_value({'something': None}, ['something', 'in', 'the', 'way'])


class FileSplit(unittest.TestCase):

    def test_file_chunks(self):
        with TmpFile() as tmp_filename:
            with open(tmp_filename, 'w') as f:
                f.truncate(10000)
            chunks = list(split_file(tmp_filename, 1000))
            self.assertEqual(len(chunks), 10, 'Incorrect number of chunks from file')

    def test_file_chunks_with_smaller_last_chunk(self):
        with TmpFile() as tmp_filename:
            with open(tmp_filename, 'w') as f:
                f.truncate(10000)
            chunks = list(split_file(tmp_filename, 3333))
            self.assertEqual(len(chunks), 4, 'Incorrect number of chunks from file')

    def test_generate_multipart_md5(self):
        with TmpFile() as tmp_filename:
            with open(tmp_filename, 'w') as f:
                f.truncate(10000)
            md5 = calculate_multipart_etag(tmp_filename, 3333)
            expected = '"728d2dbdd842b6a145cc3f3284d66861-4"'
            self.assertEqual(md5, expected, 'Wrong calculated md5 for multipart upload')


class HomogeneusListTest(unittest.TestCase):
    def test_homogeneus_lists(self):
        assert isinstance(dict_list(({}, {4: 5})), list)

        with self.assertRaises((TypeError, ValueError)):
            dict_list([{}, 'Not a dictionary'])

        with self.assertRaises((TypeError, ValueError)):
            int_list([40, 'Not an integer'])

        assert isinstance(str_list((u'a', 'b')), list)
        assert isinstance(int_list((u'1', 56)), list)

    def test_maybe_cast_list(self):
        # Don't do anything if it isn't a list
        assert maybe_cast_list(None, list) is None
        assert maybe_cast_list(50, list) == 50

        # Works when we expect normal lists
        assert type(maybe_cast_list([], list)) == list

        # Cast to more specific list subclasses whenever possible
        assert type(maybe_cast_list([4, 5], (int_list))) == int_list
        assert type(maybe_cast_list(['asd', 5], (str_list))) == str_list

        # Return original value if can't cast
        assert type(maybe_cast_list(['asd', 5], (int_list))) == list

        # Try to cast to every list subclass
        assert type(maybe_cast_list(['asd', 5], (int_list, str_list))) == str_list
