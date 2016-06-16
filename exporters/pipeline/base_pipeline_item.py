import os
import six
from exporters.exceptions import ConfigurationError
from exporters.utils import read_option, maybe_cast_list


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

    def __init__(self, options, metadata, *args, **kwargs):
        self.options = options.get('options', {})
        self.check_options()
        self.metadata = metadata

    def check_options(self):
        for option_name, option_spec in self.supported_options.iteritems():
            option_value = maybe_cast_list(self.read_option(option_name), option_spec['type'])
            if option_value and not isinstance(option_value, option_spec['type']):
                raise ConfigurationError('Value for option %s should be of type: %s' %
                                         (option_name, option_spec['type']))
            if 'default' in option_spec:
                continue
            if 'env_fallback' in option_spec and option_value is None:
                if not os.environ.get(option_spec['env_fallback']):
                    raise ConfigurationError(
                        'Missing value for option {}. (tried also: {} from env)'.format(
                            option_name, option_spec['env_fallback'])
                    )
            elif option_value is None:
                raise ConfigurationError('Missing value for option %s' % option_name)

    def read_option(self, option_name, default=None):
        return read_option(option_name, self.options, self.supported_options, default)

    def set_metadata(self, key, value, module):
        self.metadata.per_module[module][key] = value

    def update_metadata(self, d, module):
        self.metadata.per_module[module].update(d)

    def get_metadata(self, key, module):
        return self.metadata.per_module[module].get(key)

    def get_all_metadata(self, module):
        return self.metadata.per_module[module]
