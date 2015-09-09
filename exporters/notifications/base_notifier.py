class BaseNotifier(object):
    """
    This module takes care of notifications delivery. It has a slightly different architecture than the others due to the support
    of multiple notification endpoints to be loaded at the same time. As you can see in the provided example, the notifications
    parameter is an array of notification objects. To extend and add notification endpoints, they can implement the following
    methods:
    """
    def __init__(self, options, settings):
        self.options = options
        self.settings = settings
        self.requirements = getattr(self, 'requirements', {})
        self.check_options()

    def notify_start_dump(self, receivers=None, info=None):
        """
        Notifies the start of a dump to the receivers
        """
        raise NotImplementedError

    def notify_complete_dump(self, receivers=None, info=None):
        """
        Notifies the end of a dump to the receivers
        """
        raise NotImplementedError

    def notify_failed_job(self, mgs, stack_trace, receivers=None, info=None):
        """
        Notifies the failure of a dump to the receivers
        """
        raise NotImplementedError

    def check_options(self):
        options = self.options['options']
        for requirement_name, requirement_info in self.requirements.iteritems():
            if not requirement_info['required']:
                continue
            if requirement_name not in options:
                raise ValueError('Options object should have parameter: ' + requirement_name)
            elif not isinstance(options[requirement_name], requirement_info['type']):
                raise ValueError('Parameter ' + requirement_name + ' should be type: ' + str(requirement_info['type']))
