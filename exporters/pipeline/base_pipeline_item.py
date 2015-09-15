from exporters.parameterized import Parameterized
from exporters.notifications.notifiers_list import NotifiersList


class BasePipelineItem(Parameterized):

    def __init__(self, configuration, settings):
        super(BasePipelineItem, self).__init__(configuration, settings)
        self.notifiers = NotifiersList(settings)
