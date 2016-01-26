import mock
import unittest
from exporters.readers import HubstorageReader


class HubstorageReaderTest(unittest.TestCase):
    def setUp(self):
        pass

    @mock.patch('exporters.readers.hubstorage_reader.HubstorageReader._create_collection_scanner')
    def test_set_last_position_legacy(self, mock_create_scanner):
        options = dict(apikey='fake', collection_name='collection', project_id='10804')
        hs_reader = HubstorageReader(dict(options=options))
        hs_reader.set_last_position('resumekey')
        self.assertEquals([mock.call.set_startafter('resumekey')],
                          mock_create_scanner.return_value.mock_calls)
        self.assertEquals(dict(last_key='resumekey'), hs_reader.last_position)

    @mock.patch('exporters.readers.hubstorage_reader.HubstorageReader._create_collection_scanner')
    def test_set_last_position(self, mock_create_scanner):
        options = dict(apikey='fake', collection_name='collection', project_id='10804')
        hs_reader = HubstorageReader(dict(options=options))
        hs_reader.set_last_position(dict(last_key='resumekey'))
        self.assertEquals([mock.call.set_startafter('resumekey')],
                          mock_create_scanner.return_value.mock_calls)
        self.assertEquals(dict(last_key='resumekey'), hs_reader.last_position)

    @mock.patch('exporters.readers.hubstorage_reader.HubstorageReader._create_collection_scanner')
    def test_update_last_position_after_getting_batch(self, mock_create_scanner):
        mock_create_scanner.return_value.get_new_batch.side_effect = [
            [{'_key': 'value1'}, {'_key': 'value2'}],
            [{'_key': 'value3'}, {'_key': 'value4'}],
        ]

        options = dict(apikey='fake', collection_name='collection', project_id='10804')
        hs_reader = HubstorageReader(dict(options=options))
        list(hs_reader.get_next_batch())
        self.assertEquals('value2', hs_reader.last_position['last_key'])
        list(hs_reader.get_next_batch())
        self.assertEquals('value4', hs_reader.last_position['last_key'])
