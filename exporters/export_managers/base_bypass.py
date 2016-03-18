import os
import logging


class RequisitesNotMet(Exception):
    """
    Exception thrown when bypass requisites are note meet.
    """


class BaseBypass(object):
    def __init__(self, config):
        self.config = config
        self.total_items = 0
        self.valid_total_count = True

    def meets_conditions(self):
        raise NotImplementedError

    def bypass(self):
        raise NotImplementedError

    def increment_items(self, number_of_items):
        self.total_items += number_of_items

    def read_option(self, module, option, env_fallback=None):
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

        option = options.get(option, os.getenv(env_fallback))
        if not option and env_fallback:
            logging.log(logging.WARNING, 'Missing value for option {}. (tried also: {} from env)'
                        .format(option, env_fallback))
        return option
