import os


class RequisitesNotMet(Exception):
    """
    Exception thrown when bypass requisites are note meet.
    """


class BaseBypass(object):

    replace_modules = {}

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
        raise NotImplementedError

    def bypass(self):
        raise NotImplementedError

    def increment_items(self, number_of_items):
        self.total_items += number_of_items

    def _read_option(self, option, options, options_module, default=None):
        if option in options:
            return options.get(option)
        env_name = options_module.supported_options.get(option, {}).get('env_fallback')
        if env_name and env_name in os.environ:
            return os.environ.get(env_name)
        return options_module.supported_options.get(option, {}).get('default', default)

    def read_reader_option(self, option, default=None):
        options = self.config.reader_options['options']
        if self.reader_class is None:
            raise ValueError('Reader module is not being replaced, so not known options available')
        return self._read_option(option, options, self.reader_class)

    def read_writer_option(self, option, default=None):
        options = self.config.writer_options['options']
        if self.writer_class is None:
            raise ValueError('Writer module is not being replaced, so not known options available')
        return self._read_option(option, options, self.writer_class)

    def read_filter_after_option(self, option, default=None):
        options = self.config.filter_after_options['options']
        if self.filter_after_class is None:
            raise ValueError(
                    'Filer after module is not being replaced, so not known options available')
        return self._read_option(option, options, self.filter_after_class)

    def read_filter_before_option(self, option, default=None):
        options = self.config.filter_before_options['options']
        if self.filter_before_class is None:
            raise ValueError(
                    'Filter before module is not being replaced, so not known options available')
        return self._read_option(option, options, self.filter_before_class)

    def read_formatter_option(self, option, default=None):
        options = self.config.formatter_options['options']
        if self.formatter_class is None:
            raise ValueError(
                    'Formatter module is not being replaced, so not known options available')
        return self._read_option(option, options, self.formatter_class)

    def read_grouper_option(self, option, default=None):
        options = self.config.grouper_options['options']
        if self.grouper_class is None:
            raise ValueError('Grouper module is not being replaced, so not known options available')
        return self._read_option(option, options, self.grouper_class)

    def read_persistence_option(self, option, default=None):
        options = self.config.persistence_options['options']
        if self.persistence_class is None:
            raise ValueError(
                    'Persistence module is not being replaced, so not known options available')
        return self._read_option(option, options, self.persistence_class)

    def read_stats_option(self, option, default=None):
        options = self.config.stats_options['options']
        if self.stats_class is None:
            raise ValueError(
                    'Stats module is not being replaced, so not known options available')
        return self._read_option(option, options, self.stats_class)

    def read_transform_option(self, option, default=None):
        options = self.config.transform_options['options']
        if self.transform_class is None:
            raise ValueError(
                    'Transform module is not being replaced, so not known options available')
        return self._read_option(option, options, self.transform_class)
