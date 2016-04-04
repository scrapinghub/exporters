import os

import six

from exporters.defaults import DEFAULT_FILTER_CONFIG, DEFAULT_GROUPER_CONFIG
from exporters.defaults import DEFAULT_TRANSFORM_CONFIG


class ReplaceModulesMeta(type):
    def __init__(cls, *args):
        super(ReplaceModulesMeta, cls).__init__(*args)
        options = {}
        for superclass in reversed(cls.__mro__):
            if 'replace_modules' in vars(superclass):
                options.update(superclass.replace_modules)
        options.update(getattr(cls, 'replace_modules', {}))
        cls.replace_modules = options


@six.add_metaclass(ReplaceModulesMeta)
class BaseBypass(object):

    replace_modules = {}
    defaults_by_modules = {
        'reader': None,
        'writer': None,
        'transform': DEFAULT_TRANSFORM_CONFIG,
        'grouper': DEFAULT_GROUPER_CONFIG,
        'filter_before': DEFAULT_FILTER_CONFIG,
        'filter_after': DEFAULT_FILTER_CONFIG
    }

    def __init__(self, config, metadata):
        self.config = config
        self.metadata = metadata
        self.total_items = 0
        self.valid_total_count = True
        self.reader_class = self.replace_modules.get('reader')
        self.writer_class = self.replace_modules.get('writer')
        self.filter_after_class = self.replace_modules.get('filter_after')
        self.filter_before_class = self.replace_modules.get('filter_before')
        self.formatter_class = self.replace_modules.get('formatter')
        self.grouper_class = self.replace_modules.get('grouper')
        self.persistence_class = self.replace_modules.get('persistence')
        self.stats_class = self.replace_modules.get('stats')
        self.transform_class = self.replace_modules.get('transform')

    def meets_conditions(self):
        modules_to_be_default = list(
                set(self.defaults_by_modules.keys()) - set(self.replace_modules.keys()))
        for module_type, module_class in self.replace_modules.iteritems():
            if not self.is_valid_name(
                    self.config.module_options(module_type).get('name'), module_class):
                return self._handle_conditions_not_met('Wrong {} configured'.format(module_type))
        for module in modules_to_be_default:
            if not self.is_default(self.config.module_options(module).get('name'),
                                   self.defaults_by_modules[module]):
                return self._handle_conditions_not_met('custom {} configured'.format(module))

        if self.read_writer_option('items_limit', use_supported_default=False):
            return self._handle_conditions_not_met('items limit configuration (items_limit)')
        if self.read_writer_option('items_per_buffer_write', use_supported_default=False):
            return self._handle_conditions_not_met(
                    'buffer limit configuration (items_per_buffer_write)')
        if self.read_writer_option('size_per_buffer_write', use_supported_default=False):
            return self._handle_conditions_not_met(
                    'buffer limit configuration (size_per_buffer_write)')
        return True

    def bypass(self):
        raise NotImplementedError

    def _handle_conditions_not_met(self, reason):
        self.logger.warning('Skipping file copy optimization bypass because of %s' % reason)
        return False

    def increment_items(self, number_of_items):
        self.total_items += number_of_items

    def _read_option(self, option, options, options_module,
                     default=None, use_supported_default=True):
        if option in options:
            return options.get(option)
        env_name = options_module.supported_options.get(option, {}).get('env_fallback')
        if env_name and env_name in os.environ:
            return os.environ.get(env_name)
        if use_supported_default:
            return options_module.supported_options.get(option, {}).get('default', default)

    def read_reader_option(self, option, default=None, use_supported_default=True):
        options = self.config.reader_options['options']
        if self.reader_class is None:
            raise ValueError('Reader module is not being replaced, so not known options available')
        return self._read_option(
                option, options, self.reader_class, use_supported_default=use_supported_default)

    def read_writer_option(self, option, default=None, use_supported_default=True):
        options = self.config.writer_options['options']
        if self.writer_class is None:
            raise ValueError('Writer module is not being replaced, so not known options available')
        return self._read_option(
                option, options, self.writer_class, use_supported_default=use_supported_default)

    def read_filter_after_option(self, option, default=None, use_supported_default=True):
        options = self.config.filter_after_options['options']
        if self.filter_after_class is None:
            raise ValueError(
                    'Filer after module is not being replaced, so not known options available')
        return self._read_option(
                option, options, self.filter_after_class,
                use_supported_default=use_supported_default)

    def read_filter_before_option(self, option, default=None, use_supported_default=True):
        options = self.config.filter_before_options['options']
        if self.filter_before_class is None:
            raise ValueError(
                    'Filter before module is not being replaced, so not known options available')
        return self._read_option(
                option, options, self.filter_before_class,
                use_supported_default=use_supported_default)

    def read_formatter_option(self, option, default=None, use_supported_default=True):
        options = self.config.formatter_options['options']
        if self.formatter_class is None:
            raise ValueError(
                    'Formatter module is not being replaced, so not known options available')
        return self._read_option(
                option, options, self.formatter_class, use_supported_default=use_supported_default)

    def read_grouper_option(self, option, default=None, use_supported_default=True):
        options = self.config.grouper_options['options']
        if self.grouper_class is None:
            raise ValueError('Grouper module is not being replaced, so not known options available')
        return self._read_option(
                option, options, self.grouper_class, use_supported_default=use_supported_default)

    def read_persistence_option(self, option, default=None, use_supported_default=True):
        options = self.config.persistence_options['options']
        if self.persistence_class is None:
            raise ValueError(
                    'Persistence module is not being replaced, so not known options available')
        return self._read_option(
                option, options, self.persistence_class,
                use_supported_default=use_supported_default)

    def read_stats_option(self, option, default=None, use_supported_default=True):
        options = self.config.stats_options['options']
        if self.stats_class is None:
            raise ValueError(
                    'Stats module is not being replaced, so not known options available')
        return self._read_option(
                option, options, self.stats_class, use_supported_default=use_supported_default)

    def read_transform_option(self, option, default=None, use_supported_default=True):
        options = self.config.transform_options['options']
        if self.transform_class is None:
            raise ValueError(
                    'Transform module is not being replaced, so not known options available')
        return self._read_option(
                option, options, self.transform_class, use_supported_default=use_supported_default)

    def is_valid_name(self, name, module_class, default=None):
        module_name = module_class.__module__ + "." + module_class.__name__
        if name != module_name:
            return False
        return True

    def is_default(self, name, defaults):
        if name != defaults.get('name'):
            return False
        return True
