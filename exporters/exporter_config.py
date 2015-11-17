import collections
from importlib import import_module
from inspect import getmembers, isclass
import json
from exporters.exceptions import ConfigurationError
from exporters.defaults import DEFAULT_FILTER_CLASS, DEFAULT_GROUPER_CLASS, DEFAULT_PERSISTENCE_CLASS, \
    DEFAULT_STATS_MANAGER_CLASS, DEFAULT_FORMATTER_CLASS, DEFAULT_LOGGER_LEVEL, DEFAULT_LOGGER_NAME, \
    DEFAULT_TRANSFORM_CLASS


class ExporterConfig(object):
    def __init__(self, configuration):
        validate(configuration)
        self.configuration = configuration
        self.exporter_options = self.configuration['exporter_options']
        self.reader_options = self._merge_options('reader')
        if 'filter' in self.configuration:
            self.filter_before_options = self._merge_options('filter')
        else:
            self.filter_before_options = self._merge_options('filter_before', DEFAULT_FILTER_CLASS)
        self.filter_after_options = self._merge_options('filter_after', DEFAULT_FILTER_CLASS)
        self.transform_options = self._merge_options('transform', DEFAULT_TRANSFORM_CLASS)
        self.grouper_options = self._merge_options('grouper', DEFAULT_GROUPER_CLASS)
        self.writer_options = self._merge_options('writer')
        # Persistence module needs to know about the full configuration, in order to retrieve it if needed
        self.persistence_options = self._merge_options('persistence', DEFAULT_PERSISTENCE_CLASS)
        self.persistence_options['configuration'] = json.dumps(configuration)
        self.persistence_options['resume'] = configuration['exporter_options'].get('resume', False)
        self.persistence_options['persistence_state_id'] = configuration['exporter_options'].get('persistence_state_id', None)
        self.stats_options = self._merge_options('stats_manager', DEFAULT_STATS_MANAGER_CLASS)
        self.formatter_options = self.configuration['exporter_options'].get('formatter', DEFAULT_FORMATTER_CLASS)
        self.notifiers = self.configuration['exporter_options'].get('notifications', [])

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


MODULE_TYPES = ['readers', 'writers', 'transform', 'groupers',
                'persistence', 'filters', 'stats_managers']


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


REQUIRED_CONFIG_SECTIONS = frozenset(
    ['reader', 'writer', 'exporter_options'])


Parameter = collections.namedtuple('Parameter', 'name options')


def validate(config):
    missing_sections = REQUIRED_CONFIG_SECTIONS - set(config.keys())
    if missing_sections:
        raise ConfigurationError(
            "Configuration is missing sections: !{}!".format(
                ', '.join(missing_sections)))

    for section in ['reader', 'writer', 'filter', 'filter_before',
                    'filter_after', 'transform', 'persistence']:
        config_section = config.get(section)
        if config_section is not None:
            _check_valid_options(config_section, section)

    return True


def _check_valid_options(config_section, section_name):
    if 'name' not in config_section:
        raise ConfigurationError(
            'Module name for section "{}" is missing'
            .format(section_name))
    module_name = config_section['name']
    # We only check the required supported_options
    options = [Parameter(name=name, options=option_spec)
               for name, option_spec in
               _get_module_supported_options(module_name).iteritems()
               if _required_option(option_spec)]
    config_options = config_section.get('options', {})
    for option in options:
        _check_required_config_section(
            option, config_options, section_name)


def _required_option(option_spec):
    return ('default' not in option_spec and
            'env_fallback' not in option_spec)


def _check_required_config_section(option_definition, config_options,
                                   section_name):
    if option_definition.name not in config_options:
        raise ConfigurationError(
            'option "{}" for section "{}" is missing'
            .format(option_definition.name, section_name))
    if not isinstance(config_options[option_definition.name],
                      option_definition.options['type']):
        raise ConfigurationError(
            'Wrong type for option "{}". Found: {}. Expected {}'.format(
                option_definition.name, type(
                    config_options[option_definition.name]),
                option_definition.options['type']))


def _get_available_classes(module):
    classes_names = []
    for name, obj in getmembers(module):
        if isclass(obj):
            classes_names.append(obj.__module__ + '.' + obj.__name__)
    return classes_names


def _get_module_supported_options(module_name):
    try:
        class_path_list = module_name.split('.')
        mod = import_module('.'.join(class_path_list[0:-1]))
        supported_options = getattr(mod, class_path_list[-1]).supported_options
        try:
            getattr(mod, 'FilebaseBaseWriter')
            supported_options['filebase'] = {'type': basestring}
        except:
            pass
        return supported_options
    except Exception as e:
        raise ConfigurationError(
            'There was a problem loading {} class. Exception: {}'
            .format(module_name, e))
