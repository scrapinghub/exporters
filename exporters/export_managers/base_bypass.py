class RequisitesNotMet(Exception):
    """
    Exception thrown when bypass requisites are note meet.
    """


class BaseBypass(object):
    def __init__(self, config):
        self.config = config
        self.total_items = 0

    def meets_conditions(self):
        raise NotImplementedError

    def bypass(self):
        raise NotImplementedError

    def increment_items(self, number_of_items):
        self.total_items += number_of_items