import six
from importlib import import_module
import default_settings

class Settings(object):
    def __init__(self, options, module=None):
        self.attributes = {}
        self.add_module(default_settings)
        if module:
            self.add_module(module)
        if options:
            self.set_from_dict(options)

    def add_module(self, module):
        if isinstance(module, Settings):
            for name, value in module.attributes.items():
                self.set(name, value)
        else:
            if isinstance(module, six.string_types):
                module = import_module(module)
            for key in dir(module):
                if key.isupper():
                    self.set(key, getattr(module, key))

    def get(self, key, default_value=None):
        if not key.isupper():
            return None
        return self.attributes.get(key, default_value)

    def set(self, key, value):
        if key.isupper():
            self.attributes[key] = value

    def set_from_dict(self, attributes):
        for name, value in attributes.items():
            self.set(name, value)