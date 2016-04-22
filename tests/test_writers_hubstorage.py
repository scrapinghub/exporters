import mock
import unittest
from contextlib import closing

from exporters.export_formatter.json_export_formatter import JsonExportFormatter
from exporters.records.base_record import BaseRecord
from exporters.writers.hubstorage_writer import HubstorageWriter

from .utils import meta


class HubstorageWriterTest(unittest.TestCase):

    @mock.patch('hubstorage.collectionsrt.Collection')
    def test_should_push_items_to_hubstorage(self, mock_col):
        mock_writer = mock_col.return_value.create_writer.return_value
        # given:
        batch = [
            BaseRecord({'name': 'item1', 'country_code': 'es'}),
            BaseRecord({'name': 'item2', 'country_code': 'uk'}),
            BaseRecord({'name': 'item3', 'something': 'else'}),
        ]

        options = {
            "project_id": "10804",
            "collection_name": "test_collection",
            "key_field": "name",
            'apikey': 'fakeapikey',
            'size_per_buffer_write': 2,
        }

        # when:
        writer = HubstorageWriter({"options": options}, meta(),
                                  export_formatter=JsonExportFormatter(dict()))
        with closing(writer):
            writer.write_batch(batch)
            writer.flush()

        self.assertEqual(3, len(mock_writer.write.mock_calls))
        self.assertEqual(3, writer.get_metadata('items_count'))

        expected_calls = [mock.call(dict(it, _key=it['name'])) for it in batch]
        self.assertEqual(expected_calls, mock_writer.write.mock_calls)
