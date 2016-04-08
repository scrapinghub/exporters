import six
from copy import copy
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
            'help': 'Record field which should be used as Hubstorage item key'
        },
        'apikey': {'type': six.string_types, 'help': 'Hubstorage API key'}
    }

    def __init__(self, *args, **kwargs):
        super(HubstorageWriter, self).__init__(*args, **kwargs)
        self.project_id = self.read_option('project_id')
        self.collection_name = self.read_option('collection_name')
        self.key_field = self.read_option('key_field')
        self.collection = self._get_collection()
        self.logger.info('Will write items into project {}, '
                         ' collection {}'.format(self.project_id, self.collection_name))

    def write_batch(self, batch):
        super(HubstorageWriter, self).write_batch(batch)

        hs_items = []
        for item in batch:
            hs_item = copy(item)  # shallow copy should be enough
            hs_item['_key'] = item[self.key_field]
            hs_items.append(hs_item)
        self.collection.set(hs_items)

    def _get_collection(self):
        import hubstorage
        client = hubstorage.HubstorageClient(self.read_option('apikey'))
        project = client.get_project(self.project_id)
        return project.collections.new_store(self.collection_name)
