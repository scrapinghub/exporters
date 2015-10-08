import os
from exporters.exceptions import ConfigurationError


class BasePipelineItem(object):

    base_supported_options = {}
    supported_options = {}

    def __new__(cls, options):
        obj = object.__new__(cls)
        object.__init__(obj, options)
        obj.supported_options = cls.base_supported_options.copy()
        obj.supported_options.update(cls.supported_options)
        return obj

    def __init__(self, options):
        self.options = options.get('options', {})
        self.check_options()

    def check_options(self):
        for option_name, option_spec in self.supported_options.iteritems():
            option_value = self.read_option(option_name)
            if option_value and not isinstance(option_value, option_spec['type']):
                raise ConfigurationError('Option %s should be type: %s' %
                                         (option_name, option_spec['type']))
            if 'default' in option_spec:
                continue
            if 'env_fallback' in option_spec and option_value is None:
                if not os.environ.get(option_spec['env_fallback']):
                    raise ConfigurationError('Missing value for option {}. (tried also: {} from env)'
                                             .format(option_name, option_spec['env_fallback']))
            elif option_value is None:
                raise ConfigurationError('Missing value for option %s' % option_name)

    def read_option(self, option_name, default=None):
        if option_name in self.options:
            return self.options.get(option_name)
        env_name = self.supported_options.get(option_name, {}).get('env_fallback')
        if env_name and os.environ.get(env_name) is not None and os.environ.get(env_name) is not '':
            return os.environ.get(env_name)
        return self.supported_options.get(option_name, {}).get('default', default)
