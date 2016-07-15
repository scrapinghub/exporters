import six
from exporters.readers.base_reader import BaseReader
from exporters.records.base_record import BaseRecord
from exporters.utils import str_list


class HubstorageReader(BaseReader):
    """
    This reader retrieves items from Scrapinghub Hubstorage collections.

        - batch_size (int)
            Number of items to be returned in each batch

        - apikey (str)
            API key with access to the project where the items are being generated.

        - project_id (int or str)
            Id of the project.

        - collection_name (str)
            Name of the collection of items.

        - count (int)
            Number of records to read from collection.

        - prefixes (list)
            Only include records with given key prefixes.

        - exclude_prefixes (list)
            Exclude records with given key prefixes.

        - secondary_collections (list)
            A list of secondary collections to merge from.

        - startts (int or str)
            Either milliseconds since epoch, or date string.

        - endts (int or str)
            Either milliseconds since epoch, or date string.
    """

    # List of options to set up the reader
    supported_options = {
        'batch_size': {'type': six.integer_types, 'default': 10000},
        'apikey': {'type': six.string_types, 'env_fallback': 'EXPORTERS_HS_APIKEY'},
        'project_id': {'type': six.integer_types + six.string_types},
        'collection_name': {'type': six.string_types},
        'count': {'type': six.integer_types, 'default': 0},
        'prefixes': {'type': str_list, 'default': []},
        'exclude_prefixes': {'type': str_list, 'default': []},
        'secondary_collections': {'type': str_list, 'default': []},
        'has_many_collections': {'type': dict, 'default': {}},
        'startts': {'type': six.integer_types + six.string_types, 'default': None},
        'endts': {'type': six.integer_types + six.string_types, 'default': None},
    }

    def __init__(self, *args, **kwargs):
        super(HubstorageReader, self).__init__(*args, **kwargs)
        self.batch_size = self.read_option('batch_size')
        self.collection_scanner = self._create_collection_scanner()
        self.logger.info(
            'HubstorageReader has been initiated. '
            'Project id: {}. Collection name: {}'.format(
                self.read_option('project_id'), self.read_option('collection_name'))
        )
        self.last_position = {}

    def _create_collection_scanner(self):
        from collection_scanner import CollectionScanner
        return CollectionScanner(self.read_option('apikey'), str(self.read_option('project_id')),
                                 self.read_option('collection_name'),
                                 batchsize=self.batch_size,
                                 startafter=self.last_position.get('last_key', ''),
                                 count=self.read_option('count'),
                                 prefix=self.read_option('prefixes'),
                                 exclude_prefixes=self.read_option('exclude_prefixes'),
                                 secondary_collections=self.read_option('secondary_collections'),
                                 has_many_collections=self.read_option('has_many_collections'),
                                 startts=self.read_option('startts'),
                                 endts=self.read_option('endts'),
                                 meta=['_key'])

    def get_next_batch(self):
        """
        This method is called from the manager. It must return a list or a generator
        of BaseRecord objects.
        When it has nothing else to read, it must set class variable "finished" to True.
        """
        if self.collection_scanner.is_enabled:
            batch = self.collection_scanner.get_new_batch()
            for item in batch:
                base_item = BaseRecord(item)
                self.increase_read()
                self.last_position['last_key'] = item['_key']
                yield base_item
            self.logger.debug('Done reading batch')
        else:
            self.logger.debug('No more batches')
            self.finished = True

    def set_last_position(self, last_position):
        """
        Called from the manager, it is in charge of updating the last position of data commited
        by the writer, in order to have resume support
        """
        if last_position:
            if isinstance(last_position, six.string_types):
                last_key = last_position
            else:
                last_key = last_position.get('last_key', '')
            self.last_position = dict(last_key=last_key)
            self.collection_scanner.set_startafter(last_key)
        else:
            self.last_position = {}
