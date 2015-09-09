from requests.auth import HTTPBasicAuth
import requests
import yaml
from exporters.export_managers.base_exporter import BaseExporter
from exporters.export_managers.bypass import S3Bypass
from exporters.exporter_options import ExporterOptions
from exporters.persistence.persistence_config_dispatcher import PersistenceConfigDispatcher


class UnifiedExporter(BaseExporter):
    """
    Dataservices export manager reading configuration from api url. It adds bypass support.
    """

    def __init__(self, configuration):
        super(UnifiedExporter, self).__init__(configuration)

    @staticmethod
    def from_url_configuration(url, apikey):
        auth = HTTPBasicAuth(apikey, '')
        headers = {'Content-type': 'application/json'}
        conf_string = requests.get(url, headers=headers, auth=auth).content
        return UnifiedExporter(yaml.safe_load(conf_string))

    @staticmethod
    def from_file_configuration(filepath):
        conf_string = open(filepath).read()
        return UnifiedExporter(yaml.safe_load(conf_string))

    @staticmethod
    def from_persistence_configuration(persistence_uri):
        conf_string = PersistenceConfigDispatcher(persistence_uri).config
        return UnifiedExporter(conf_string)

    @property
    def bypass_cases(self):
        config = ExporterOptions(self.configuration)
        return [S3Bypass(config)]
