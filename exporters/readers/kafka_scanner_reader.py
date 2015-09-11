"""
Kafka reader
"""
from retrying import retry
from exporters.readers.base_reader import BaseReader
from exporters.records.base_record import BaseRecord
from kafka_scanner import KafkaScanner


class KafkaScannerReader(BaseReader):
    """
    Reads items from kafka brokers.

    Needed parameters:

        - batch_size (int)
            Number of items to be returned in each batch

        - brokers (list)
            List of brokers uris.

        - topic (str)
            Topic to read from.

        - group (str)
            Reading group for kafka client.
    """

     # List of required options to set up the reader
    parameters = {
        'batch_size': {'type': int, 'default': 10000},
        'brokers': {'type': list},
        'topic': {'type': basestring},
        'group': {'type': basestring}
    }

    def __init__(self, options, settings):
        super(KafkaScannerReader, self).__init__(options, settings)

        brokers = self.read_option('brokers')
        group = self.read_option('group')
        topic = self.read_option('topic')
        scanner = KafkaScanner(brokers, topic, group,
                batchsize=self.read_option('batch_size'), keep_offsets=settings.get('RESUME'))

        self.batches = scanner.scan_topic_batches()

        self.logger.info('KafkaScannerReader has been initiated. Topic: {}. Group: {}'.format(self.read_option('topic'),
                                                                                             self.read_option('group')))

    @retry(wait_exponential_multiplier=500, wait_exponential_max=10000, stop_max_attempt_number=10)
    def get_from_kafka(self):
        return self.batches.next()

    def get_next_batch(self):
        try:
            batch = self.get_from_kafka()
            for message in batch:
                item = BaseRecord(message)
                yield item
        except:
            self.finished = True
        self.logger.debug('Done reading batch')

    def set_last_position(self, last_position):
        if last_position is None:
            self.last_position = {}
        else:
            self.last_position = last_position
