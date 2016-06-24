import logging

from exporters_bloom_filter.filter import DuplicatesBloomFilter
from exporters.meta import ExportMeta
from exporters.readers import RandomReader
from exporters.writers import ConsoleWriter
logging.basicConfig()


if __name__ == '__main__':

    # Exporters modules need a shared meta object, to store shared information
    meta = ExportMeta(None)

    # As this is just an example, we will leave modules with default options. For more options
    # documentation please refer to project docs
    reader_1 = RandomReader({}, meta)
    reader_2 = RandomReader({}, meta)
    writer = ConsoleWriter({}, meta)
    # Using a Bloom filter to detect duplicates is easy and memory efficient. Please take a look
    # at its project https://github.com/jaybaird/python-bloomfilter/
    filter = DuplicatesBloomFilter({'options': {'field': 'city'}}, meta)

    readers = [reader_1, reader_2]

    def all_readers_finished():
        return all(reader.is_finished() for reader in readers)

    # We keep looping until all readers have finished
    while not all_readers_finished():
        for reader in readers:
            if not reader.is_finished():
                batch = reader.get_next_batch()
                filtered_batch = filter.filter_batch(batch)
                writer.write_batch(filtered_batch)
