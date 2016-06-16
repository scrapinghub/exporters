import collections
from importlib import import_module
from inspect import getmembers, isclass
import json
from exporters.utils import maybe_cast_list
from exporters.exceptions import ConfigCheckError
from exporters.defaults import (
    DEFAULT_FILTER_CONFIG, DEFAULT_GROUPER_CONFIG, DEFAULT_PERSISTENCE_CONFIG,
    DEFAULT_STATS_MANAGER_CCONFIG, DEFAULT_FORMATTER_CONFIG, DEFAULT_LOGGER_LEVEL,
    DEFAULT_LOGGER_NAME, DEFAULT_TRANSFORM_CONFIG
)


class ExporterConfig(object):
    def __init__(self, configuration):
        check_for_errors(configuration)
        self.configuration = configuration
        exporter_options = self.configuration.get('exporter_options', {})
        self.exporter_options = exporter_options
        self.reader_options = self._merge_options('reader')
        if 'filter' in self.configuration:
            self.filter_before_options = self._merge_options('filter')
        else:
            self.filter_before_options = self._merge_options('filter_before', DEFAULT_FILTER_CONFIG)
        self.filter_after_options = self._merge_options('filter_after', DEFAULT_FILTER_CONFIG)
        self.transform_options = self._merge_options('transform', DEFAULT_TRANSFORM_CONFIG)
        self.grouper_options = self._merge_options('grouper', DEFAULT_GROUPER_CONFIG)
        self.writer_options = self._merge_options('writer')
        # Persistence module needs to know about the full configuration,
        # in order to retrieve it if needed
        self.persistence_options = self._merge_options('persistence', DEFAULT_PERSISTENCE_CONFIG)
        self.persistence_options.update(
            configuration=json.dumps(configuration),
            resume=exporter_options.get('resume', False),
            persistence_state_id=exporter_options.get('persistence_state_id', None)
        )
        self.stats_options = self._merge_options('stats_manager', DEFAULT_STATS_MANAGER_CCONFIG)
        self.formatter_options = exporter_options.get('formatter', DEFAULT_FORMATTER_CONFIG)
        self.notifiers = exporter_options.get('notifications', [])

    def __str__(self):
        return json.dumps(self.configuration)

    def _merge_options(self, module_name, default=None):
        options = self.configuration.get(module_name, default)
        options.update(self.log_options)
        return options

    @property
    def log_options(self):
        return {'log_level': self.exporter_options.get('log_level', DEFAULT_LOGGER_LEVEL),
                'logger_name': self.exporter_options.get('logger_name', DEFAULT_LOGGER_NAME)}

    @property
    def prevent_bypass(self):
        return self.exporter_options.get('prevent_bypass', False)

    @property
    def disable_retries(self):
        return self.exporter_options.get('disable_retries', False)

    def get_supported_options(self, module_type):
        options_name = '{}_options'.format(module_type)
        if not hasattr(self, options_name):
            raise ValueError("Invalid config section: %r" % options_name)
        name = getattr(self, options_name)['name']
        return _get_module_supported_options(name)


MODULE_TYPES = ['readers', 'writers', 'transform', 'groupers',
                'persistence', 'filters', 'stats_managers', 'export_formatter',
                'notifications']


def module_options():
    options = {}
    for module_type in MODULE_TYPES:
        module = import_module('exporters.{}'.format(module_type))
        classes = _get_available_classes(module)
        class_infos = [
            {'name': clazz,
             'options': _get_module_supported_options(clazz)}
            for clazz in classes]
        options[module_type] = class_infos

    return options


REQUIRED_CONFIG_SECTIONS = frozenset(['reader', 'writer'])


Parameter = collections.namedtuple('Parameter', 'name options')


def check_for_errors(config, raise_exception=True):
    """
    Returns config validation errors if raise_exception is False,
    otherwise raises ConfigurationError with those errors in it.
    Errors are represented as nested dicts (sections & options in them).
    """
    errors = {}
    for section in ['reader', 'writer', 'filter', 'filter_before',
                    'filter_after', 'transform', 'persistence']:
        config_section = config.get(section)
        if config_section is None:
            if section in REQUIRED_CONFIG_SECTIONS:
                errors[section] = "Missing section"
        else:
            section_errors = _get_section_errors(config_section)
            if section_errors:
                errors[section] = section_errors

    exporter_options = config.get('exporter_options', {})
    if "formatter" in exporter_options:
        section_errors = _get_section_errors(exporter_options['formatter'])
        if section_errors:
            errors['formatter'] = section_errors

    for i, notificator in enumerate(exporter_options.get('notifications', [])):
        section_errors = _get_section_errors(notificator)
        if section_errors:
            errors['notifications_' + str(i)] = section_errors

    if raise_exception and errors:
        raise ConfigCheckError(errors=errors)
    else:
        return errors


def _get_section_errors(config_section):
    if 'name' not in config_section:
        return 'Module name is missing'
    module_name = config_section['name']
    try:
        module_options = _get_module_supported_options(module_name)
        option_errors = {}
        config_options = config_section.get('options', {})
        for name, spec in module_options.iteritems():
            error = _get_option_error(name, spec, config_options)
            if error:
                option_errors[name] = error
        not_supported_options = list(set(config_options.keys()) - set(module_options.keys()))
        if not_supported_options:
            option_errors['unsupported_options'] = not_supported_options
        return option_errors
    except ConfigCheckError as e:
        return e.message  # in general we should check e.errors also


def _required_option(option_spec):
    return ('default' not in option_spec and
            'env_fallback' not in option_spec)


class empty:
    pass


def _get_option_error(name, spec, config_options):
    required = 'default' not in spec and 'env_fallback' not in spec

    if required and name not in config_options:
        return 'Option %s is missing' % name
    else:
        value = maybe_cast_list(config_options.get(name, empty), spec['type'])
        if value is not empty and not isinstance(value, spec['type']):
            return 'Wrong type: found {}, expected {}'.format(
                type(value), spec['type'])
    return None


def _get_available_classes(module):
    classes_names = set()
    for name, obj in getmembers(module):
        if isclass(obj):
            classes_names.add(obj.__module__ + '.' + obj.__name__)
    return classes_names


def _get_module_supported_options(module_name):
    try:
        class_path_list = module_name.split('.')
        mod = import_module('.'.join(class_path_list[0:-1]))
        supported_options = getattr(mod, class_path_list[-1]).supported_options
        return supported_options
    except Exception as e:
        raise ConfigCheckError(
            message='There was a problem loading {} class, exception: {}'
            .format(module_name, e))
