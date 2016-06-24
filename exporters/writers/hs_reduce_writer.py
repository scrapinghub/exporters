import re
from .reduce_writer import ReduceWriter
from exporters.exceptions import ConfigurationError
from collections import MutableMapping
import six


COLLECTION_REGEX = '.*[.]scrapinghub[.]com/p/(\d+)/collections/s/([^/]+)/?$'


class HubstorageReduceWriter(ReduceWriter):

    """
    This writer allow exporters to make aggregation of items data and push results into
    Scrapinghub Hubstorage collections

        - code (str)
            Python code defining a reduce_function(item, accumulator=None)

        - collection_url (str)
            Hubstorage Collection URL

        - key (str)
            Element key where to push the accumulated result

        - apikey (dict)
            Hubstorage API key
    """

    supported_options = {
        "collection_url": {
            'type': six.string_types,
            'help': 'Hubstorage Collection URL'
        },
        'key': {
            'type': six.string_types,
            'help': 'Element key where to push the accumulated result'
        },
        'apikey': {'type': six.string_types, 'help': 'Hubstorage API key'},
    }
    supported_options.update(ReduceWriter.supported_options)

    def __init__(self, *args, **kwargs):
        super(HubstorageReduceWriter, self).__init__(*args, **kwargs)
        self.collection = self._get_collection()
        self.element_key = self.read_option('key')
        collection_url = self.read_option('collection_url')
        self.logger.info('Will write accumulator to: {}#/details/{}'
                         .format(collection_url, self.element_key))

    def get_result(self, **extra):
        result = dict(self.reduced_result
                      if isinstance(self.reduced_result, MutableMapping)
                      else dict(value=self.reduced_result))
        result['_key'] = self.element_key
        result.update(extra)
        return result

    def write_batch(self, batch):
        super(HubstorageReduceWriter, self).write_batch(batch)
        self.collection.set(self.get_result(_finished=False))

    def finish_writing(self):
        self.collection.set(self.get_result(_finished=True))

    def _get_collection(self):
        collection_url = self.read_option('collection_url')
        match = re.match(COLLECTION_REGEX, collection_url)
        if not match:
            raise ConfigurationError("Invalid collection_url: %s" % collection_url)
        project, collection_name = match.groups()

        import hubstorage
        client = hubstorage.HubstorageClient(self.read_option('apikey'))
        return client.get_project(project).collections.new_store(collection_name)
