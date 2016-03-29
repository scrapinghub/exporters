#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Random items generator, just for testing purposes
"""
from exporters.readers.base_reader import BaseReader
import random
from exporters.records.base_record import BaseRecord


class RandomReader(BaseReader):
    """
    It is just a reader with testing purposes. It generates random data in a quantity that
    is set in its config section.

        - number_of_items (int)
            Number of total items that must be returned by the reader before finishing.

        - batch_size (int)
            Number of items to be returned in each batch.
    """

    supported_options = {
        'number_of_items': {'type': int, 'default': 1000},
        'batch_size': {'type': int, 'default': 100}
    }

    def __init__(self, *args, **kwargs):
        super(RandomReader, self).__init__(*args, **kwargs)
        self.last_read = self.last_position.get('last_read', -1)
        self.logger.info('RandomReader has been initiated')
        self.country_codes = [u'es', u'uk', u'us']
        self.states = [u'valéncia', u'madrid', u'barcelona']
        self.cities = [
            {'name': u'alicante', 'district': u'dist1'},
            {'name': u'alicante', 'district': u'dist2'},
            {'name': u'alicante', 'district': u'dist3'},
            {'name': u'lléida', 'district': u'dist1'},
            {'name': u'somecity', 'district': u'dist1'}]
        self.batch_size = self.read_option('batch_size')

    def get_next_batch(self):
        """
        This method is called from the manager. It must return a list or a generator
        of BaseRecord objects.
        When it has nothing else to read, it must set class variable "finished" to True.
        """
        number_of_items = self.read_option('number_of_items')
        for i in range(0, self.batch_size):
            to_read = self.last_read + 1
            if to_read >= number_of_items:
                self.finished = True
                break
            else:
                item = BaseRecord()
                self.last_read = to_read
                item['key'] = self.last_read
                item['country_code'] = random.choice(self.country_codes)
                item['state'] = random.choice(self.states)
                item['city'] = random.choice(self.cities)
                item['value'] = random.randint(0, 10000)
                self.increase_read()
                self.last_position['last_read'] = self.last_read
                yield item
        self.logger.debug('Done reading batch')

    def set_last_position(self, last_position):
        """
        Called from the manager, it is in charge of updating the last position of data commited
        by the writer, in order to have resume support
        """
        self.last_position = last_position
        if last_position is not None and last_position.get('last_read') is not None:
            self.last_read = last_position['last_read']
        else:
            self.last_read = -1
            self.last_position = {
                'last_read': self.last_read
            }
