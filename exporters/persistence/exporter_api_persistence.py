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
        return json.loads(requests.get(url).text, auth=self.auth)

    def _put_request(self, url, data):
        return json.loads(requests.put(url, data=data).text, auth=self.auth)

    def _post_request(self, url, data):
        return json.loads(requests.post(url, data=data).text, auth=self.auth)

    def position(self, position_id):
        url = '{}{}'.format(self.url, position_id)
        return self._get_request(url)

    def update_position(self, position_id, position, **kwargs):
        url = '{}{}'.format(self.url, position_id)
        data = kwargs
        data['last_position'] = json.dumps(position)
        data['type'] = type(position).__name__
        return self._put_request(url, data)

    def create_position(self, last_position, position_type, configuration):
        data = {
            'last_position': json.dumps(last_position),
            'type': str(position_type),
            'configuration': json.dumps(configuration)
        }
        return self._post_request(self.url, data)


class ExporterApiPersistence(BasePersistence):

    supported_options = {
        'apikey': {'type': basestring},
        'api_url': {'type': basestring, 'default': 'https://datahub-exports-api.scrapinghub.com/exports_persistence/'}
    }

    uri_regex = "sh_exporter:(([a-zA-Z\d-]|\/)+)"

    def __init__(self, options):
        api_url = self.read_option('api_url')
        apikey = self.read_option('apikey')
        self.api_client = ApiClient(api_url, apikey)
        super(ExporterApiPersistence, self).__init__(options)

    def get_last_position(self):
        position = self.api_client.position(self.job_id)
        self.last_position = position.get('last_position')
        if self.last_position == 'null':
            self.last_position = None
        else:
            try:
                exec('self.last_position = {}({})'.format(position['type'], position['last_position']))
            except Exception as e:
                print e
        return self.last_position

    def commit_position(self, last_position=None):
        self.last_position = last_position
        self.api_client.update_position(self.job_id, self.last_position)
        self.logger.debug('Commited batch number ' + str(self.last_position) + ' of job: ' + str(self.job_id))

    def generate_new_job(self):
        self.last_position = None
        persistence_object = self.api_client.create_position(None, type(self.last_position).__name__, self.configuration)
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
