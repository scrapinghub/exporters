import exporters.readers as reader_classes
import exporters.writers as writer_classes
import exporters.transform as transform_classes
import exporters.groupers as groupers_classes
import exporters.persistence as persistence_classes
import exporters.filters as filter_classes

from importlib import import_module
from inspect import getmembers, isclass
import collections


Parameter = collections.namedtuple('Parameter', 'name options')


class InvalidConfigError(RuntimeError):
    pass


class ConfigApi(object):
    required_sections = ('reader', 'writer', 'persistence', 'exporter_options')

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

    def _get_available_classes(self, module):
        classes_names = []
        for name, obj in getmembers(module):
            if isclass(obj):
                classes_names.append(obj.__module__ + '.' + obj.__name__)
        return classes_names

    def get_module_requirements(self, module_name):
        try:
            class_path_list = module_name.split('.')
            mod = import_module('.'.join(class_path_list[0:-1]))
        except Exception as e:
            raise InvalidConfigError('There was a problem loading {} class. Exception: {}'.format(module_name, e))
        requirements = getattr(mod, class_path_list[-1]).requirements
        return requirements

    def check_valid_config(self, config):
        if self._find_missing_sections(config):
            raise InvalidConfigError(
                "Configuration is missing sections: %s" % ', '.join(self._find_missing_sections(config)))
        self._check_valid_reader(config['reader'])
        self._check_valid_writer(config['writer'])
        if 'filter' in config:
            self._check_valid_filter(config['filter'])
        if 'filter_before' in config:
            self._check_valid_filter(config['filter_before'])
        if 'filter_after' in config:
            self._check_valid_filter(config.get('filter_after'))
        if 'transform' in config:
            self._check_valid_transform(config['transform'])
        self._check_valid_persistence(config['persistence'])
        return True

    def _find_missing_sections(self, config):
        return set(self.required_sections) - set(config.keys())

    def _check_required_config_section(self, parameter, config_section):
        if parameter.name not in config_section['options']:
            raise InvalidConfigError('{} parameter is missing'.format(parameter.name))
        if not isinstance(config_section['options'][parameter.name], parameter.options['type']):
            raise InvalidConfigError(
                'Wrong type for parameter {}. Found: {}. Expected {}'.format(parameter.name, type(
                    config_section['options'][parameter.name]), parameter.options['type']))

    def _check_valid_requirements(self, config_section):
        if 'name' not in config_section or 'options' not in config_section:
            raise InvalidConfigError('Module has not name or options parameter')
        # We only check the required parameters
        parameters = [Parameter(name=r_name, options=r_info) for r_name, r_info in self.get_module_requirements(config_section['name']).iteritems() if r_info.get('required')]
        for parameter in parameters:
            self._check_required_config_section(parameter, config_section)

    # We keep different checkers, to support different check methods
    def _check_valid_reader(self, config_section):
        self._check_valid_requirements(config_section)

    def _check_valid_writer(self, config_section):
        self._check_valid_requirements(config_section)
        if 'grouper' in config_section:
            self._check_valid_requirements(config_section['grouper'])

    def _check_valid_filter(self, config_section):
        self._check_valid_requirements(config_section)

    def _check_valid_grouper(self, config_section):
        self._check_valid_requirements(config_section)

    def _check_valid_transform(self, config_section):
        self._check_valid_requirements(config_section)

    def _check_valid_persistence(self, config_section):
        self._check_valid_requirements(config_section)
