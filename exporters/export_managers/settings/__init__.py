from UserDict import UserDict
from . import default_settings


class Settings(UserDict):
    def __init__(self, options):
        UserDict.__init__(self)
        self.load_defaults()
        if options:
            self.update(options)

    def load_defaults(self):
        for key in dir(default_settings):
            if key and not key.startswith('_'):
                self[key.lower()] = getattr(default_settings, key)
