from exporters.filters.base_filter import BaseFilter
from pybloom import BloomFilter
import copy
from exporters.records.base_record import BaseRecord


def make_hash(o):
    if isinstance(o, (set, tuple, list)):
        return tuple([make_hash(e) for e in o])

    elif not isinstance(o, dict):
        return hash(o)

    new_o = copy.deepcopy(o)
    for k, v in new_o.items():
        new_o[k] = make_hash(v)

    return hash(tuple(frozenset(sorted(new_o.items()))))


class DuplicatesBloomFilter(BaseFilter):
    """
    Filter that removes duplicated items

        - capacity (int)
            Filter capacity

        - error_rate(float)
            Desired error rate
    """
    # List of options
    supported_options = {
        'capacity': {'type': int, 'default': 1000000},
        'error_rate': {'type': float, 'default': 0.001},
        'field': {'type': basestring, 'default': None},
    }

    def __init__(self, *args, **kwargs):
        super(DuplicatesBloomFilter, self).__init__(*args, **kwargs)
        self.field = self.read_option('field')
        self.bloom_filter = BloomFilter(
                capacity=self.read_option('capacity'), error_rate=self.read_option('error_rate'))
        self.logger.info('BloomFilter has been initiated.')

    def filter(self, item):
        if self.field:
            item_hash = make_hash(item.get(self.field, ''))
        else:
            item_hash = make_hash(item)
        if not item_hash in self.bloom_filter:
            self.bloom_filter.add(item_hash)
            return True
        return False
