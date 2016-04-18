import datetime
from collections import defaultdict
from copy import deepcopy


class ExportMeta(object):
    def __init__(self, configuration, start_time=None):
        self.configuration = configuration
        self.start_time = start_time or datetime.datetime.now()
        self.end_time = None
        self.accurate_items_count = True
        self.bypassed_pipeline = False
        self.per_module = defaultdict(dict)

    @property
    def elapsed_time(self):
        if self.end_time:
            return self.end_time - self.start_time
        else:
            return None

    def to_dict(self):
        d = deepcopy(self.per_module)
        d.update({
            'configuration': self.configuration,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'elapsed_time': self.elapsed_time,
            'bypassed_pipeline': self.bypassed_pipeline,
            'accurate_items_count': self.accurate_items_count,
        })
        return d
