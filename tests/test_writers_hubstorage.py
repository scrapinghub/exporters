import json
import responses
import unittest
from copy import deepcopy

from exporters.export_formatter.json_export_formatter import JsonExportFormatter
from exporters.records.base_record import BaseRecord
from exporters.writers.hubstorage_writer import HubstorageWriter

from .utils import meta


class HubstorageWriterTest(unittest.TestCase):
    def test_should_push_items_to_hubstorage(self):
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
            'apikey': 'fakeapikey'
        }

        # when:
        with responses.RequestsMock(assert_all_requests_are_fired=False) as r:
            r.add(responses.POST,
                  "http://storage.scrapinghub.com/collections/10804/s/test_collection",
                  body='{}')
            writer = HubstorageWriter({"options": options}, meta(),
                                      export_formatter=JsonExportFormatter(dict()))
            writer.write_batch(batch)
            written = [json.loads(l) for l in r.calls[0].request.body.split('\n')]

        # then:
        self.assertEqual(3, writer.get_metadata('items_count'))
        expected_items = deepcopy(batch)
        for item in expected_items:
            item['_key'] = item['name']
        self.assertEqual(written, expected_items)

        writer.close()
