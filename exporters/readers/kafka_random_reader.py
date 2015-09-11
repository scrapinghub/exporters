"""
Kafka random reader
"""
import zlib
import msgpack
from retrying import retry
import kafka
import random

from exporters.readers.base_reader import BaseReader
from exporters.records.base_record import BaseRecord
from kafka_scanner.msg_processor import MsgProcessor


class KafkaRandomReader(BaseReader):
    """
    Reads a random subset of items from kafka brokers.

    Needed parameters:

        - record_count (int)
            Number of items to be returned in total

        - batch_size (int)
            Number of items to be returned in each batch

        - brokers (list)
            List of brokers uris.

        - topic (str)
            Topic to read from.

        - group (str)
            Reading group for kafka client.
    """

    parameters = {
        'record_count': {'type': int},
        'batch_size': {'type': int, 'default': 10000},
        'brokers': {'type': list},
        'topic': {'type': basestring},
        'group': {'type': basestring}
    }

    def __init__(self, options, settings):
        super(KafkaRandomReader, self).__init__(options, settings)

        brokers = self.read_option('brokers')
        group = self.read_option('group')
        topic = self.read_option('topic')

        client = kafka.KafkaClient(map(bytes, brokers))

        # TODO: Remove this comments when next steps are decided.
        # If resume is set to true, then child should not load initial offsets
        # child_loads_initial_offsets = False if settings.get('RESUME') else True

        # self.consumer = kafka.MultiProcessConsumer(client, group, topic, num_procs=1,
        #                                             child_loads_initial_offsets=child_loads_initial_offsets,
        #                                             auto_commit=False)

        self.consumer = kafka.SimpleConsumer(client, group, topic,
                                             auto_commit=False)

        self.decompress_fun = zlib.decompress
        self.processor = self.create_processor()
        self.partitions = client.get_partition_ids_for_topic(topic)

        self.logger.info('KafkaRandomReader has been initiated. Topic: {}. Group: {}'.format(self.read_option('topic'),
                                                                                             self.read_option('group')))

        self.logger.info('Running random sampling')
        self._reservoir = self.fill_reservoir()
        self.logger.info('Random sampling completed, ready to process batches')

    def _reservoir_sample(self, reservoir, index, record, count):
        if index < count:
            reservoir.append(record)
        else:
            r = random.randint(0, index)
            if r < count:
                reservoir[r] = record

    def fill_reservoir(self):
        batch_size = self.read_option('batch_size')
        record_count = self.read_option('record_count')
        index = 0
        reservoir = []
        while self.consumer.pending():
            for record in self.consumer.get_messages(batch_size):
                self._reservoir_sample(reservoir, index, record, record_count)
                index += 1
        return reservoir

    @retry(wait_exponential_multiplier=500, wait_exponential_max=10000, stop_max_attempt_number=10)
    def get_from_kafka(self):
        batch_size = self.read_option('batch_size')
        return self.processor.process(batch_size)

    def get_next_batch(self):
        messages = self.get_from_kafka()
        if messages:
            for message in messages:
                item = BaseRecord(message)
                yield item

        self.logger.debug('Done reading batch')
        self.last_position = self.consumer.offsets

    def create_processor(self):
        processor = MsgProcessor()
        # NOTE This is the order we want the functions to run:
        # each of it (except last) should return a generator.
        # You can stop the process using bool self.enabled var.
        processor.add_handler(self.consume_messages)
        processor.add_handler(self.decompress_messages)
        processor.add_handler(self.unpack_messages)
        return processor

    def consume_messages(self, batchsize):
        """ Get messages batch from the reservoir """
        if not self._reservoir:
            self.finished = True
            return
        for msg in self._reservoir[:batchsize]:
            yield msg
        self._reservoir = self._reservoir[batchsize:]

    def decompress_messages(self, offmsgs):
        """ Decompress pre-defined compressed fields for each message.
            Msgs should be unpacked before this step. """

        for offmsg in offmsgs:
            yield offmsg.message.key, self.decompress_fun(offmsg.message.value)

    @staticmethod
    def unpack_messages(msgs):
        """ Deserialize a message to python structures """

        for key, msg in msgs:
            record = msgpack.unpackb(msg)
            record['_key'] = key
            yield record

    def set_last_position(self, last_position):
        if last_position is None:
            self.last_position = {}
            for partition in self.partitions:
                self.last_position[partition] = 0
            self.consumer.offsets = self.last_position.copy()
            self.consumer.fetch_offsets = self.consumer.offsets.copy()
        else:
            self.last_position = last_position
            self.consumer.offsets = last_position.copy()
            self.consumer.fetch_offsets = self.consumer.offsets.copy()
