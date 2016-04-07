import gzip
import json
import six

import requests
from exporters.default_retries import retry_short, retry_long

from exporters.writers.base_writer import BaseWriter


CLOUDSEARCH_MAX_BATCH_SIZE = 5 * 1000 * 1000


def create_document_batches(jsonlines, id_field, max_batch_size=CLOUDSEARCH_MAX_BATCH_SIZE):
    """Create batches in expected AWS Cloudsearch format, limiting the
    byte size per batch according to given max_batch_size

    See: http://docs.aws.amazon.com/cloudsearch/latest/developerguide/preparing-data.html
    """
    batch = []
    fixed_initial_size = 2

    def create_entry(line):
        try:
            record = json.loads(line)
        except:
            raise ValueError('Could not parse JSON from: %s' % line)
        key = record[id_field]
        return '{"type":"add","id":%s,"fields":%s}' % (json.dumps(key), line)

    current_size = fixed_initial_size
    for line in jsonlines:
        entry = create_entry(line)
        entry_size = len(entry) + 1

        if max_batch_size > (current_size + entry_size):
            current_size += entry_size
            batch.append(entry)
        else:
            yield '[' + ','.join(batch) + ']'
            batch = [entry]
            current_size = fixed_initial_size + entry_size

    if batch:
        yield '[' + ','.join(batch) + ']'


class CloudSearchWriter(BaseWriter):
    """
    This writer stores items in CloudSearch Amazon Web Services service
    (https://aws.amazon.com/es/cloudsearch/)

    - endpoint_url
        Document Endpoint (e.g.: http://doc-movies-123456789012.us-east-1.cloudsearch.amazonaws.com)

    - id_field
        Field to use as identifier

    - access_key
        Public acces key to the s3 bucket.

    - secret_key
        Secret access key to the s3 bucket.
    """

    supported_options = {
        'endpoint_url': {
            'type': six.string_types,
            'help': 'Document Endpoint'
            ' (e.g.: http://doc-movies-123456789012.us-east-1.cloudsearch.amazonaws.com)'
        },
        'id_field': {
            'type': six.string_types,
            'help': 'Field to use as identifier',
            'default': '_key',
        },
        'access_key': {
            'type': six.string_types,
            'env_fallback': 'EXPORTERS_CLOUDSEARCH_ACCESS_KEY',
            'default': None,
        },
        'secret_key': {
            'type': six.string_types,
            'env_fallback': 'EXPORTERS_CLOUDSEARCH_SECRET_KEY',
            'default': None,
        },
    }

    def __init__(self, options, *args, **kwargs):
        super(CloudSearchWriter, self).__init__(options, *args, **kwargs)
        self.endpoint_url = self.read_option('endpoint_url').rstrip('/')
        self.access_key = self.read_option('access_key')
        self.secret_key = self.read_option('secret_key')
        self.id_field = self.read_option('id_field')

    @retry_short
    def _post_document_batch(self, batch):
        """
        Send a batch to Cloudsearch endpoint

        See: http://docs.aws.amazon.com/cloudsearch/latest/developerguide/submitting-doc-requests.html
        """  # noqa
        target_batch = '/2013-01-01/documents/batch'
        url = self.endpoint_url + target_batch
        return requests.post(url, data=batch, headers={'Content-type': 'application/json'})

    @retry_long
    def write(self, dump_path, group_key=None):
        with gzip.open(dump_path) as f:
            for batch in create_document_batches(iter(f), self.id_field):
                self.logger.info('Starting upload batch with size %d' % len(batch))
                self._post_document_batch(batch)
                self.logger.info('Uploaded batch with size %d' % len(batch))
