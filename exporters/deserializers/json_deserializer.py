import json
from exporters.records.base_record import BaseRecord
from exporters.deserializers.base_deserializer import BaseDeserializer


class JsonLinesDeserializer(BaseDeserializer):
    def deserialize(self, stream):
        for line in stream.iterlines():
            yield BaseRecord(json.loads(line))
