import yaml

from exporters.bypasses import default_bypass_classes
from exporters.persistence.persistence_config_dispatcher import PersistenceConfigDispatcher
from .base_exporter import BaseExporter


class BasicExporter(BaseExporter):
    """
    Basic export manager reading configuration a json file. It has bypass support.
    """

    def __init__(self, configuration):
        super(BasicExporter, self).__init__(configuration)
        self.bypass_cases = default_bypass_classes

    @classmethod
    def from_file_configuration(cls, filepath):
        conf_string = open(filepath).read()
        return cls(yaml.safe_load(conf_string))

    @classmethod
    def from_persistence_configuration(cls, persistence_uri):
        conf_string = PersistenceConfigDispatcher(persistence_uri).config
        return cls(conf_string)
