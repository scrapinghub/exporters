"""
Kafka reader
"""
from exporters.readers.base_reader import BaseReader
from exporters.records.base_record import BaseRecord
from exporters.default_retries import retry_short
from exporters.utils import str_list


class KafkaScannerReader(BaseReader):
    """
    This reader retrieves items from kafka brokers.

        - batch_size (int)
            Number of items to be returned in each batch

        - brokers (list)
            List of brokers uris.

        - topic (str)
            Topic to read from.

        - partitions (list)
            Partitions to read from.

        - group (str)
            Reading group for kafka client.
    """

    # List of options to set up the reader
    supported_options = {
        'batch_size': {'type': int, 'default': 10000},
        'brokers': {'type': str_list},
        'topic': {'type': basestring},
        'group': {'type': basestring},
        'partitions': {'type': str_list, 'default': None}
    }

    def __init__(self, *args, **kwargs):
        from kafka_scanner import KafkaScanner, KafkaScannerSimple
        super(KafkaScannerReader, self).__init__(*args, **kwargs)
        brokers = self.read_option('brokers')
        group = self.read_option('group')
        topic = self.read_option('topic')
        partitions = self.read_option('partitions')
        if partitions and len(partitions) == 1:
            scanner_class = KafkaScannerSimple
        else:
            scanner_class = KafkaScanner

        scanner = scanner_class(brokers, topic, group, partitions=partitions,
                                batchsize=self.read_option('batch_size'),
                                keep_offsets=self.read_option('RESUME'))

        self.batches = scanner.scan_topic_batches()

        if partitions:
            topic_str = '{} (partitions: {})'.format(topic, partitions)
        else:
            topic_str = topic
        self.logger.info('KafkaScannerReader has been initiated.'
                         'Topic: {}. Group: {}'.format(topic_str, group))

    @retry_short
    def get_from_kafka(self):
        return self.batches.next()

    def get_next_batch(self):
        """
        This method is called from the manager. It must return a list or a generator
        of BaseRecord objects.
        When it has nothing else to read, it must set class variable "finished" to True.
        """
        try:
            batch = self.get_from_kafka()
            for message in batch:
                item = BaseRecord(message)
                self.increase_read()
                yield item
        except:
            self.finished = True
        self.logger.debug('Done reading batch')

    def set_last_position(self, last_position):
        """
        Called from the manager, it is in charge of updating the last position of data commited
        by the writer, in order to have resume support
        """
        if last_position is None:
            self.last_position = {}
        else:
            self.last_position = last_position
