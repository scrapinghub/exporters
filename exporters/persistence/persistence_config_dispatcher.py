from __future__ import absolute_import
import re
from exporters.persistence import PERSISTENCE_LIST
import six


class PersistenceConfigDispatcher(object):

    def __init__(self, uri):
        self.uri = uri
        self.persistence_dispatcher = self.get_module_from_uri()

    def get_module_from_uri(self):
        persistence_regexes = {m.uri_regex: m for m in PERSISTENCE_LIST}

        for regex, module in six.iteritems(persistence_regexes):
            if re.match(regex, self.uri):
                return module

        raise ValueError('{} is not a valid persistence uri. Available handlers are {}.'
                         .format(self.uri, [m.uri_regex for m in PERSISTENCE_LIST]))

    @property
    def config(self):
        return self.persistence_dispatcher.configuration_from_uri(
            self.uri, self.persistence_dispatcher.uri_regex)
