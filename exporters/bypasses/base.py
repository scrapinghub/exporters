import os
import logging


class BaseBypass(object):
    def __init__(self, config, metadata):
        self.config = config
        self.metadata = metadata
        self.total_items = 0
        self.valid_total_count = True
        self.logger = logging.getLogger('bypass_logger')
        self.logger.setLevel(logging.INFO)
        self.supported_options = {module_name: self.config.get_supported_options(module_name)
                                  for module_name in
                                  ['reader', 'writer', 'transform', 'grouper', 'persistence',
                                   'filter_after', 'filter_before', 'stats']}

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
        options_name = '{}_options'.format(module)
        options = getattr(self.config, options_name)['options']
        env_fallback = self.supported_options[module][option].get('env_fallback')
        option = options.get(option, os.getenv(env_fallback))
        if not option and env_fallback:
            logging.log(logging.WARNING, 'Missing value for option {}. (tried also: {} from env)'
                        .format(option, env_fallback))
        return option

    def close(self):
        pass
