from exporters.readers.base_reader import BaseReader
from exporters.records.base_record import BaseRecord


class HubstorageReader(BaseReader):
    """
    This reader retrieves items from Scrapinghub Hubstorage collections.

        - batch_size (int)
            Number of items to be returned in each batch

        - apikey (str)
            API key with access to the project where the items are being generated.

        - project_id (str)
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
    """

    # List of options to set up the reader
    supported_options = {
        'batch_size': {'type': int, 'default': 10000},
        'apikey': {'type': basestring, 'env_fallback': 'EXPORTERS_HS_APIKEY'},
        'project_id': {'type': basestring},
        'collection_name': {'type': basestring},
        'count': {'type': int, 'default': 0},
        'prefixes': {'type': list, 'default': []},
        'exclude_prefixes': {'type': list, 'default': []},
        'secondary_collections': {'type': list, 'default': []},
    }

    def __init__(self, options):
        from collection_scanner import CollectionScanner
        super(HubstorageReader, self).__init__(options)
        self.batch_size = self.read_option('batch_size')
        self.collection_scanner = CollectionScanner(self.read_option('apikey'), self.read_option('project_id'),
                                                    self.read_option('collection_name'),
                                                    batchsize=self.batch_size,
                                                    startafter=self.last_position,
                                                    count=self.read_option('count'),
                                                    prefix=self.read_option('prefixes'),
                                                    exclude_prefixes=self.read_option('exclude_prefixes'),
                                                    secondary_collections=self.read_option('secondary_collections'),
                                                    meta=['_key'])
        self.logger.info('HubstorageReader has been initiated. Project id: {}. Collection name: {}'.format(
            self.read_option('project_id'), self.read_option('collection_name')))
        self.last_position = ''

    def get_next_batch(self):
        """
        This method is called from the manager. It must return a list or a generator of BaseRecord objects.
        When it has nothing else to read, it must set class variable "finished" to True.
        """
        if self.collection_scanner.is_enabled:
            batch = self.collection_scanner.get_new_batch()
            for item in batch:
                base_item = BaseRecord(item)
                self.stats['read_items'] += 1
                self.last_position = item['_key']
                yield base_item
            self.logger.debug('Done reading batch')
        else:
            self.logger.debug('No more batches')
            self.finished = True

    def set_last_position(self, last_position):
        """
        Called from the manager, it is in charge of updating the last position of data commited by the writer, in order to
        have resume support
        """
        if last_position:
            self.last_position = last_position
            self.collection_scanner.set_startafter(last_position)
        else:
            self.last_position = ''
