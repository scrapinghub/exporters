import json
import requests
from exporters.writer import BaseWriter

CLOUDSEARCH_MAX_BATCH_SIZE = 5 * 1000 * 1000


def create_document_batches(data, batch_size=CLOUDSEARCH_MAX_BATCH_SIZE):
    """Create batches limiting to size
    """
    raise NotImplementedError()


class CloudSearchWriter(BaseWriter):
    supported_options = {
        'endpoint_url': {
            'type': basestring,
            'help_text': 'Document Endpoint'
            ' (e.g.: http://doc-movies-123456789012.us-east-1.cloudsearch.amazonaws.com)'
        },
        'access_key': {
            'type': basestring,
            'env_fallback': 'EXPORTERS_CLOUDSEARCH_ACCESS_KEY',
        },
        'secret_key': {
            'type': basestring,
            'env_fallback': 'EXPORTERS_CLOUDSEARCH_SECRET_KEY',
        },
    }

    def __init__(self, options):
        super(CloudSearchWriter, self).__init__(self, options)
        self.endpoint_url = self.read_option('endpoint_url').rstrip('/')
        self.access_key = self.read_option('access_key')
        self.secret_key = self.read_option('secret_key')

    def _post_document_batch(self, batch):
        """Make POST request like:
        curl -X POST --upload-file data1.json $ENDPOINT_URL/2013-01-01/documents/batch \
            --header "Content-Type: application/json"
        """
        target_batch = '/2013-01-01/documents/batch'
        url = self.endpoint_url + target_batch
        # requests.post(url, body=batch)
        raise NotImplementedError()

    def write(self, dump_path, group_key=None):
        with open(dump_path) as f:
            for batch in create_document_batches(l for l in f):
                self._post_document_batch(batch)
