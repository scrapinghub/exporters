import os
import logging
from exporters.exporter_config import CONFIG_SECTIONS


class BaseBypass(object):
    def __init__(self, config, metadata):
        self.config = config
        self.metadata = metadata
        self.total_items = 0
        self.valid_total_count = True
        self.logger = logging.getLogger('bypass_logger')
        self.logger.setLevel(logging.INFO)
        self.supported_options = {module_name: self.config.get_supported_options(module_name)
                                  for module_name in CONFIG_SECTIONS}

    @classmethod
    def meets_conditions(self, config):
        raise NotImplementedError

    def execute(self):
        raise NotImplementedError

    def increment_items(self, number_of_items):
        self.total_items += number_of_items

    @classmethod
    def _log_skip_reason(cls, reason):
        logging.debug('Skipped bypass {} due to: {}'.format(cls.__name__, reason))

    def read_option(self, module, option):
        if module == 'reader':
            options = self.config.reader_options['options']
        elif module == 'writer':
            options = self.config.writer_options['options']
        elif module == 'filter_after':
            options = self.config.filter_after_options['options']
        elif module == 'filter_before':
            options = self.config.filter_before_options['options']
        elif module == 'formatter':
            options = self.config.formatter_options['options']
        elif module == 'grouper':
            options = self.config.grouper_options['options']
        elif module == 'persistence':
            options = self.config.persistence_options['options']
        elif module == 'stats':
            options = self.config.stats_options['options']
        elif module == 'transform':
            options = self.config.transform_options['options']

        env_fallback = self.supported_options[module][option].get('env_fallback')
        option = options.get(option, os.getenv(env_fallback))
        if not option and env_fallback:
            logging.log(logging.WARNING, 'Missing value for option {}. (tried also: {} from env)'
                        .format(option, env_fallback))
        return option

    def close(self):
        pass
