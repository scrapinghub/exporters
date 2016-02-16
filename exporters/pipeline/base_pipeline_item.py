import os
import six
from exporters.exceptions import ConfigurationError


class SupportedOptionsMeta(type):
    def __init__(cls, *args):
        super(SupportedOptionsMeta, cls).__init__(*args)
        options = {}
        for superclass in reversed(cls.__mro__):
            if 'supported_options' in vars(superclass):
                options.update(superclass.supported_options)
        options.update(getattr(cls, 'supported_options', {}))
        cls.supported_options = options


@six.add_metaclass(SupportedOptionsMeta)
class BasePipelineItem(object):
    supported_options = {}

    def __init__(self, options):
        self.options = options.get('options', {})
        self.check_options()
        self.stats = {}
        self.export_metadata = {}

    def check_options(self):
        for option_name, option_spec in self.supported_options.iteritems():
            option_value = self.read_option(option_name)
            if option_value and not isinstance(option_value, option_spec['type']):
                raise ConfigurationError('Value for option %s should be of type: %s' %
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
        if env_name and env_name in os.environ:
            return os.environ.get(env_name)
        return self.supported_options.get(option_name, {}).get('default', default)
