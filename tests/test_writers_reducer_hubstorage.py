import unittest

import vcr

from exporters.records.base_record import BaseRecord
from exporters.writers.hs_reduce_writer import HubstorageReduceWriter

DASH_URL = 'https://dash.scrapinghub.com'


class HubstorageReduceWriterTest(unittest.TestCase):

    @vcr.use_cassette('tests/fixtures/vcr_cassettes/reducer_hubstorage.yaml',
                      filter_headers=['authorization'],
                      filter_query_parameters=['apikey'])
    def test_should_reduce_and_push_accumulator_to_hubstorage(self):
        # given:
        batch = [
            BaseRecord({'name': 'item1', 'country_code': 'es'}),
            BaseRecord({'name': 'item2', 'country_code': 'uk'}),
            BaseRecord({'name': 'item3', 'something': 'else'}),
        ]
        reduce_code = """
def reduce_function(item, accumulator=None):
    accumulator = 0 if accumulator is None else accumulator
    return accumulator + len(item)
        """
        collection_url = "%s/p/10804/collections/s/test_collection" % DASH_URL
        options = {
            "code": reduce_code,
            "collection_url": collection_url,
            "key": "0004",
            'apikey': 'fakeapikey'
        }
        writer = HubstorageReduceWriter({"options": options})

        # when:
        writer.write_batch(batch)

        # then:
        self.assertEqual(6, writer.reduced_result)
        self.assertEqual({'value': 6}, writer.collection.get("0004"))
        writer.close()
