class RequisitesNotMet(Exception):
    """
    Exception thrown when bypass requisites are note meet.
    """


class BaseBypass(object):
    def __init__(self, config):
        self.config = config

    def meets_conditions(self):
        raise NotImplementedError

    def bypass(self):
        raise NotImplementedError