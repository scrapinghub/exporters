from retrying import retry
from exporters.readers.base_reader import BaseReader
from exporters.records.base_record import BaseRecord
from collection_scanner import CollectionScanner


class HubstorageReader(BaseReader):
    """
    Reads items from Hubstorage collections.

    Needed parameters:

        - batch_size (int)
            Number of items to be returned in each batch

        - apikey (str)
            API key with access to the project.

        - project_id (str)
            Id of the project.

        - collection_name (str)
            Name of the collection.
    """

    # List of options to set up the reader
    parameters = {
        'batch_size': {'type': int, 'default': 10000},
        'apikey': {'type': basestring},
        'project_id': {'type': basestring},
        'collection_name': {'type': basestring}
    }

    def __init__(self, options):
        super(HubstorageReader, self).__init__(options)
        self.batch_size = self.read_option('batch_size')
        self.collection_scanner = CollectionScanner(self.read_option('apikey'), self.read_option('project_id'),
                                                    self.read_option('collection_name'),
                                                    batchsize=self.batch_size,
                                                    startafter=self.last_position)
        self.batches = self.collection_scanner.scan_collection_batches()
        self.logger.info('HubstorageReader has been initiated. Project id: {}. Collection name: {}'.format(
            self.read_option('project_id'), self.read_option('collection_name')))
        self.last_position = 0

    @retry(wait_exponential_multiplier=500, wait_exponential_max=10000, stop_max_attempt_number=10)
    def scan_collection(self):
        return self.batches.next()

    def get_next_batch(self):
        batch = self.scan_collection()
        try:
            for item in batch:
                base_item = BaseRecord(item)
                self.last_position += 1
                yield base_item
        except StopIteration:
            self.finished = True
        self.logger.debug('Done reading batch')

    def set_last_position(self, last_position):
        if last_position:
            self.last_position = last_position
        else:
            self.last_position = 0
