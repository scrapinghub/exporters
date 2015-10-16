import json
import unittest

import mock

from exporters.writers.cloudsearch_writer import (CLOUDSEARCH_MAX_BATCH_SIZE,
                                                  create_document_batches)
from exporters.export_managers.basic_exporter import BasicExporter


class CreateDocumentBatches(unittest.TestCase):

    def test_create_batch_simple(self):
        # given:
        data = [
            dict(key=1, value=2),
            dict(key=3, value=4),
            dict(key=5, value=6),
        ]
        jsonlines = [json.dumps(d) for d in data]

        # when:
        result = list(create_document_batches(jsonlines, 'key'))

        # then:
        expected = [
            [
                {'type': 'add', 'id': 1, 'fields': dict(key=1, value=2)},
                {'type': 'add', 'id': 3, 'fields': dict(key=3, value=4)},
                {'type': 'add', 'id': 5, 'fields': dict(key=5, value=6)},
            ]
        ]
        self.assertEqual(len(expected), len(result))
        for expected_batch, resulting_batch in zip(expected, result):
            self.assertEquals(expected_batch, json.loads(resulting_batch))
            self.assertLessEqual(len(resulting_batch), CLOUDSEARCH_MAX_BATCH_SIZE)

    def test_create_batch_with_size_limit(self):
        # given:
        data = [
            dict(key="1", value=2),
            dict(key="3", value=4),
            dict(key="5", value=6),
        ]
        jsonlines = [json.dumps(d) for d in data]
        max_batch_size = 150

        # when:
        result = list(create_document_batches(jsonlines, 'key', max_batch_size=max_batch_size))

        # then:
        expected = [
            [
                {'type': 'add', 'id': "1", 'fields': dict(key="1", value=2)},
                {'type': 'add', 'id': "3", 'fields': dict(key="3", value=4)},
            ],
            [
                {'type': 'add', 'id': "5", 'fields': dict(key="5", value=6)},
            ]
        ]
        self.assertEqual(len(expected), len(result))
        for expected_batch, resulting_batch in zip(expected, result):
            self.assertEquals(expected_batch, json.loads(resulting_batch))
            self.assertLessEqual(len(resulting_batch), max_batch_size)

class CloudsearchWriterTest(unittest.TestCase):
    @mock.patch('exporters.writers.cloudsearch_writer.requests', autospec=True)
    def test_run_exporter_integration(self, mock_requests):
        # given:
        endpoint_url = "http://fake-domain.us-west-2.cloudsearch.amazonaws.com"
        config = {
            "label": "unittest",
            "exporter_options": {
                "formatter": {
                    "name": "exporters.export_formatter.json_export_formatter.JsonExportFormatter",
                    "options": {}
                },
                "log_level": "INFO",
                "logger_name": "export-pipeline",
            },
            "reader": {
                "name": "exporters.readers.random_reader.RandomReader",
                "options": {
                    "number_of_items": 100
                }
            },
            "transform": {
                "name": "exporters.transform.jq_transform.JQTransform",
                "options": {
                    "jq_filter": "{key: .key, country: .country_code, value: .value} | del(.[] | select(. == null))"
                }
            },
            "writer": {
                "name": "exporters.writers.cloudsearch_writer.CloudSearchWriter",
                "options": {
                    "endpoint_url": endpoint_url,
                    "id_field": "key"
                }
            }
        }

        # when:
        exporter = BasicExporter(config)
        exporter.export()

        # then:
        self.assertEqual(1, len(mock_requests.post.mock_calls))
        url = endpoint_url + '/2013-01-01/documents/batch'
        mock_requests.post.assert_called_once_with(url, data=mock.ANY, headers=mock.ANY)
