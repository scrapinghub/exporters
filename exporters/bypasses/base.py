import logging

from exporters.utils import read_option


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
        return read_option(option, options, self.supported_options[module])

    def close(self):
        pass
