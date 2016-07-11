import csv
from exporters.records.base_record import BaseRecord
from exporters.deserializers.base import BaseDeserializer


class CSVDeserializer(BaseDeserializer):
    def deserialize(self, stream):
        reader = csv.DictReader(stream)
        for item in reader:
            yield BaseRecord(item)
