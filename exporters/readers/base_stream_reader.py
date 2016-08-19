import six
from exporters.default_retries import retry_generator
from exporters.readers.base_reader import BaseReader
from exporters.iterio import cohere_stream
from exporters.decompressors import ZLibDecompressor
from exporters.deserializers import JsonLinesDeserializer


class StreamBasedReader(BaseReader):
    """
    Abstract readers for storage backends that operate in bytes
    instead of json objects.

    The bytes are first decompressed using a decompressor and then
    deserialized using a deserializer.

    Avaliable Options:
        - batch_size (int)
            Number of items to be returned in each batch
    """

    # List of options to set up the reader
    supported_options = {
        'batch_size': {'type': six.integer_types, 'default': 10000},
    }

    def __init__(self, *args, **kwargs):
        super(StreamBasedReader, self).__init__(*args, **kwargs)
        self.iterator = None
        self.batch_size = self.read_option('batch_size')

    decompressor = ZLibDecompressor({}, None)
    deserializer = JsonLinesDeserializer({}, None)

    @retry_generator
    def iteritems_retrying(self, stream_data):
        if stream_data in self.last_position['readed_streams']:
            return
        stream = cohere_stream(self.open_stream(stream_data))
        try:
            stream = self.decompressor.decompress(stream)
            stream = cohere_stream(stream)
            items_readed = 0
            stream_offset = self.last_position['stream_offset']
            items_offset = stream_offset.get(stream_data, 0)
            for item in self.deserializer.deserialize(stream):
                items_readed += 1
                if items_readed > items_offset:
                    stream_offset[stream_data] = items_readed
                    yield item
        finally:
            stream.close()
        self.last_position['readed_streams'].append(stream_data)
        del stream_offset[stream_data]

    def iteritems(self):
        for stream in self.get_read_streams():
            for record in self.iteritems_retrying(stream):
                yield record
        self.finished = True

    def get_next_batch(self):
        """
        This method is called from the manager. It must return a list or a generator
        of BaseRecord objects.
        When it has nothing else to read, it must set class variable "finished" to True.
        """
        if self.iterator is None:
            self.iterator = self.iteritems()

        count = 0
        while count < self.batch_size:
            count += 1
            yield next(self.iterator)
        self.logger.debug('Done reading batch')

    def get_read_streams(self):
        """
        To be subclassed
        """
        raise NotImplementedError()

    def set_last_position(self, last_position):
        """
        Called from the manager, it is in charge of updating the last position of data commited
        by the writer, in order to have resume support
        """
        last_position = last_position or {}
        last_position.setdefault('readed_streams', [])
        last_position.setdefault('stream_offset', {})
        self.last_position = last_position


def is_stream_reader(reader):
    return isinstance(reader, StreamBasedReader)
