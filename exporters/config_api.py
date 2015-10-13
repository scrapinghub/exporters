import copy
import exporters.readers as reader_classes
import exporters.writers as writer_classes
import exporters.transform as transform_classes
import exporters.groupers as groupers_classes
import exporters.persistence as persistence_classes
import exporters.filters as filter_classes
import exporters.stats_managers as stats_managers
from importlib import import_module
from inspect import getmembers, isclass
import collections
from incompatibility_rules import incompatibility_rules

Parameter = collections.namedtuple('Parameter', 'name options')


class InvalidConfigError(RuntimeError):
    pass


class IncompatibleModulesUsedError(ValueError):
    pass


class ConfigApi(object):
    required_sections = ('reader', 'writer', 'exporter_options')

    @property
    def readers(self):
        return self._get_available_classes(reader_classes)

    @property
    def writers(self):
        return self._get_available_classes(writer_classes)

    @property
    def transforms(self):
        return self._get_available_classes(transform_classes)

    @property
    def groupers(self):
        return self._get_available_classes(groupers_classes)

    @property
    def persistence(self):
        return self._get_available_classes(persistence_classes)

    @property
    def filters(self):
        return self._get_available_classes(filter_classes)

    @property
    def stats_managers(self):
        return self._get_available_classes(stats_managers)

    def _get_available_classes(self, module):
        classes_names = []
        for name, obj in getmembers(module):
            if isclass(obj):
                classes_names.append(obj.__module__ + '.' + obj.__name__)
        return classes_names

    def get_module_supported_options(self, module_name):
        try:
            class_path_list = module_name.split('.')
            mod = import_module('.'.join(class_path_list[0:-1]))
            supported_options = getattr(mod, class_path_list[-1]).supported_options
            return supported_options
        except Exception as e:
            raise InvalidConfigError(
                'There was a problem loading {} class. Exception: {}'.format(module_name,
                                                                             e))

    def check_valid_config(self, config):
        if self._find_missing_sections(config):
            raise InvalidConfigError(
                "Configuration is missing sections: %s" % ', '.join(
                    self._find_missing_sections(config)))
        self._check_valid_options(config['reader'])
        self._check_valid_options(config['writer'])
        if 'filter' in config:
            self._check_valid_options(config['filter'])
        if 'filter_before' in config:
            self._check_valid_options(config['filter_before'])
        if 'filter_after' in config:
            self._check_valid_options(config.get('filter_after'))
        if 'transform' in config:
            self._check_valid_options(config['transform'])
        if 'persistence' in config:
            self._check_valid_options(config['persistence'])
        return True

    def _find_missing_sections(self, config):
        return set(self.required_sections) - set(config.keys())

    def _check_required_config_section(self, option_definition, config_section):
        if option_definition.name not in config_section['options']:
            raise InvalidConfigError(
                '{} option_definition is missing'.format(option_definition.name))
        if not isinstance(config_section['options'][option_definition.name],
                          option_definition.options['type']):
            raise InvalidConfigError(
                'Wrong type for option {}. Found: {}. Expected {}'.format(
                    option_definition.name, type(
                        config_section['options'][option_definition.name]),
                    option_definition.options['type']))

    def _check_valid_options(self, config_section):
        if 'name' not in config_section or 'options' not in config_section:
            raise InvalidConfigError('Module is missing name or option definitions')
        # We only check the required supported_options
        supported_options = (Parameter(name=name, options=option_spec)
                             for name, option_spec in
                             self.get_module_supported_options(config_section['name']).iteritems()
                             if 'default' not in option_spec)
        for supported_option in supported_options:
            self._check_required_config_section(supported_option, config_section)

    def _get_exporter_options_modules(self, exporter_options):
        modules = []
        for notification_module in exporter_options.get('notifications', []):
            modules.append(notification_module['name'])
        if 'formatter' in exporter_options:
            modules.append(exporter_options['formatter'].get('name'))
        return modules

    def _get_used_modules(self, config):
        config_copy = copy.deepcopy(config)
        modules = []
        exporter_options = config_copy.get('exporter_options', {})
        config_copy.pop('exporter_options')
        for section_name, section_options in config_copy.iteritems():
            modules.append(section_options.get('name'))
        modules += self._get_exporter_options_modules(exporter_options)
        return modules

    def _remove_from_exporter_options(self, exporter_options, module):
        for notification in exporter_options.get('notifications', []):
            if notification['name'] == module:
                exporter_options['notifications'].remove(notification)
        if 'formatter' in exporter_options and exporter_options['formatter']['name'] == module:
            exporter_options.pop('formatter')
        return exporter_options

    def _remove_from_config(self, config, module):
        config['exporter_options'] = self._remove_from_exporter_options(config.get('exporter_options'), module)
        for section in [section for section in config.keys() if section != 'exporter_options']:
            if config.get('name') == module:
                config.pop(section)

    def _check_rules(self, config):
        modules = self._get_used_modules(config)
        for module in modules:
            if module in incompatibility_rules:
                for incompatibility in incompatibility_rules.get(module):
                    if incompatibility.get('name') in modules and incompatibility.get('action') == 'fail':
                        raise IncompatibleModulesUsedError
                    elif incompatibility.get('name') in modules and incompatibility.get('action') == 'ignore':
                        self._remove_from_config(config, incompatibility.get('name'))
        return config

    def check_compatibility(self, config):
        new_config = self._check_rules(config)
        return new_config
