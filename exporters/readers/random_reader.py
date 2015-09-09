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
    It is just a reader with testing purposes. It generates random data in a quantity that is set in its config section.

    Needed parameters:

        - number_of_items (int)
            Number of total items that must be returned by the reader before finishing.

        - batch_size (int)
            Number of items to be returned in each batch
    """

    requirements = {
        'number_of_items': {'type': int, 'required': False, 'default': 1000},
        'batch_size': {'type': int, 'required': False, 'default': 100}
    }

    def __init__(self, options, settings):
        super(RandomReader, self).__init__(options, settings)
        self.last_key = self.last_position * self.read_option('batch_size')
        self.logger.info('RandomReader has been initiated')
        self.country_codes = ['es', 'uk', 'us']
        self.states = ['valéncia', 'madrid', 'barcelona']
        self.cities = [
            {'name': 'alicante', 'district': 'dist1'},
            {'name': 'alicante', 'district': 'dist2'},
            {'name': 'alicante', 'district': 'dist3'},
            {'name': 'lléida', 'district': 'dist1'},
            {'name': 'somecity', 'district': 'dist1'}]
        self.batch_size = self.read_option('batch_size')

    def get_next_batch(self):
        number_of_items = self.read_option('number_of_items')

        for i in range(0, self.batch_size):
            if self.last_key >= number_of_items:
                self.finished = True
                break
            else:
                self.last_key += 1
                item = BaseRecord()
                item['key'] = self.last_key
                item['country_code'] = random.choice(self.country_codes)
                item['state'] = random.choice(self.states)
                item['city'] = random.choice(self.cities)
                item['value'] = random.randint(0, 10000)
                yield item
        self.logger.debug('Done reading batch')
        self.last_position += 1

    def set_last_position(self, last_position):
        self.last_position = last_position
        if last_position:
            self.last_key = self.last_position * self.read_option('batch_size')
        else:
            self.last_key = 0
            self.last_position = 0