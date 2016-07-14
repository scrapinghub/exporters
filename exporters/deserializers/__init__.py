from exporters.pipeline.base_pipeline_item import BasePipelineItem
from exporters.records.base_record import BaseRecord
import json
import csv

__all__ = ['BaseDeserializer', 'JsonLinesDeserializer', 'CSVDeserializer']


class BaseDeserializer(BasePipelineItem):
    def deserialize(self, stream):
        raise NotImplementedError()


class JsonLinesDeserializer(BaseDeserializer):
    def deserialize(self, stream):
        for line in stream.iterlines():
            yield BaseRecord(json.loads(line))


class CSVDeserializer(BaseDeserializer):
    def deserialize(self, stream):
        stream.mode = "lines"
        reader = csv.DictReader(stream)
        for item in reader:
            yield BaseRecord(item)
