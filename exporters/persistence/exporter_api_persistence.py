import json
import re
from requests.auth import HTTPBasicAuth
import yaml
from exporters.persistence.base_persistence import BasePersistence
import requests


class ApiClient(object):

    def __init__(self, api_url, apikey):
        self.url = api_url
        self.auth = HTTPBasicAuth(apikey, '')

    def _get_request(self, url):
        return json.loads(requests.get(url, auth=self.auth).text)

    def _put_request(self, url, data):
        return json.loads(requests.put(url, data=data, auth=self.auth).text)

    def _post_request(self, url, data):
        return json.loads(requests.post(url, data=data, auth=self.auth).text)

    def position(self, position_id):
        url = '{}{}'.format(self.url, position_id)
        return self._get_request(url)

    def position_by_export_id(self, position_id):
        url = '{}?export_job_id={}'.format(self.url, position_id)
        response = self._get_request(url)
        if response.get('count', 0) == 1:
            return response['results'][0]

    def update_position(self, position_id, position, **kwargs):
        url = '{}{}'.format(self.url, position_id)
        data = kwargs
        data['last_position'] = json.dumps(position)
        return self._put_request(url, data)

    def create_position(self, last_position, configuration, export_id=None):
        data = {
            'last_position': json.dumps(last_position),
            'configuration': json.dumps(configuration)
        }
        if export_id:
            data['export_job_id'] = export_id
        return self._post_request(self.url, data)


class ExporterApiPersistence(BasePersistence):
    """
    This module handles persistence using exporters API. It has three working modes:

    1.- Standalone: No "extra" options are added to configurations. This way, the export
    job will run as usual. It will create a new persistence object in api database, and it
    will log its id in order to allow resume.

    2.- Start job from api: This mode is detected when a export_job_id is passed in configuration.
    It will allow the module to keep that id for api resuming support.

    3.- Resume job from api: This mode is detected when we have proper configuration in export
    options (resume set to true, and a job_id passed) AND we have apimode passed as true
    in configuration. This way, job_id from export options will be considered as a export_job_id,
    and not as the persistence id itself, allowing the module to retrieve the correct
    position instance.
    """


    supported_options = {
        'apikey': {'type': basestring},
        'export_job_id': {'type': basestring, 'default': None},
        'apimode': {'type': bool, 'default': False},
        'api_url': {'type': basestring, 'default': 'https://datahub-exports-api.scrapinghub.com/exports_persistence/'}
    }

    uri_regex = "sh_exporter:(([a-zA-Z\d-]|\/)+)"

    def __init__(self, options):
        self.last_position = None
        super(ExporterApiPersistence, self).__init__(options)
        api_url = self.read_option('api_url')
        apikey = self.read_option('apikey')
        self.api_client = self._get_api_client(api_url, apikey)
        self.export_job_id = self.read_option('export_job_id')
        self.apimode = self.read_option('apimode')

    def _get_api_client(self, api_url, apikey):
        if hasattr(self, 'api_client'):
            return self.api_client
        self.api_client = ApiClient(api_url, apikey)
        return self.api_client

    def _get_job_id_from_export_id(self):
        api_url = self.read_option('api_url')
        apikey = self.read_option('apikey')
        api_client = self._get_api_client(api_url, apikey)
        position = api_client.position_by_export_id(self.job_id)
        return position['id']

    def get_last_position(self):
        if not self.last_position and self.read_option('apimode'):
            self.job_id = self._get_job_id_from_export_id()
            api_url = self.read_option('api_url')
            apikey = self.read_option('apikey')
            api_client = self._get_api_client(api_url, apikey)
            position = api_client.position(self.job_id)
        else:
            api_url = self.read_option('api_url')
            apikey = self.read_option('apikey')
            api_client = self._get_api_client(api_url, apikey)
            position = api_client.position(self.job_id)
        self.last_position = position.get('last_position')
        if self.last_position == 'null':
            self.last_position = None
        else:
            self.last_position = json.loads(position['last_position'])
        return self.last_position

    def commit_position(self, last_position=None):
        self.last_position = last_position
        self.api_client.update_position(self.job_id, self.last_position)
        self.logger.debug('Commited batch number ' + str(self.last_position) + ' of job: ' + str(self.job_id))

    def generate_new_job(self):
        api_url = self.read_option('api_url')
        apikey = self.read_option('apikey')
        api_client = self._get_api_client(api_url, apikey)
        self.last_position = None
        if 'export_job_id' in self.options:
            persistence_object = api_client.create_position(None, self.configuration, self.options.get('export_job_id'))
        else:
            persistence_object = api_client.create_position(None, self.configuration)
        self.logger.debug('Created persistence export entry in with id {}'.format(persistence_object['id']))
        return persistence_object['id']

    def delete_instance(self):
        self.api_client.update_position(self.job_id, self.last_position, job_finished=True)

    @staticmethod
    def configuration_from_uri(uri, uri_regex):
        """
        returns a configuration object.
        """
        job_id = re.match(uri_regex, uri).groups()[0]
        api_client = ApiClient()
        position = api_client.position(job_id)
        configuration = yaml.safe_load(position.get('configuration'))
        configuration['exporter_options']['resume'] = True
        configuration['exporter_options']['job_id'] = job_id
        return configuration
