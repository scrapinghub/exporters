import six
from exporters.writers.base_writer import BaseWriter


class HubstorageWriter(BaseWriter):
    """
    This writer sends items into Scrapinghub Hubstorage collection.

        - apikey (str)
            API key with access to the project where the items are being generated.

        - project_id (str)
            Id of the project.

        - collection_name (str)
            Name of the collection of items.

        - key_field (str)
            Record field which should be used as Hubstorage item key
    """

    # List of options to set up the writer
    supported_options = {
        "project_id": {
            'type': six.string_types,
            'help': 'Id of the project'
        },
        "collection_name": {
            'type': six.string_types,
            'help': 'Name of the collection of items'
        },
        'key_field': {
            'type': six.string_types,
            'default': '_key',
            'help': 'Record field which should be used as Hubstorage item key'
        },
        'apikey': {
            'type': six.string_types,
            'help': 'Hubstorage API key',
            'env_fallback': 'EXPORTERS_HS_APIKEY'
        }
    }

    def __init__(self, *args, **kwargs):
        super(HubstorageWriter, self).__init__(*args, **kwargs)
        self.project_id = self.read_option('project_id')
        self.collection_name = self.read_option('collection_name')
        self.key_field = self.read_option('key_field')
        self.collection = self._get_collection()
        self.collection_writer = self.collection.create_writer()
        self.logger.info('Will write items into project {}, '
                         ' collection {}'.format(self.project_id, self.collection_name))

    def write_batch(self, batch):
        for item in batch:
            item_key = item[self.key_field]
            self.collection_writer.write(dict(item, _key=item_key))
            self.increment_written_items()
            self._check_items_limit()

    def flush(self):
        self.collection_writer.flush()

    def _get_collection(self):
        import hubstorage
        client = hubstorage.HubstorageClient(self.read_option('apikey'))
        project = client.get_project(self.project_id)
        return project.collections.new_store(self.collection_name)
