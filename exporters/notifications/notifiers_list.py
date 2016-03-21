from exporters.module_loader import ModuleLoader


class NotifiersList(object):
    """
    This class is only used to support a list of notifications modules.
    """

    def __init__(self, options, metadata):
        self.options = options
        self.module_loader = ModuleLoader()
        self.notifiers = self._populate_notifiers(metadata)

    def _populate_notifiers(self, metadata):
        notifiers_list = []
        for notifier in self.options:
            notifier_object = self.module_loader.load_notifier(notifier, metadata)
            notifiers_list.append(notifier_object)
        return notifiers_list

    def notify_start_dump(self, receivers=None):
        if receivers is None:
            receivers = []
        for notifier in self.notifiers:
            notifier.notify_start_dump(receivers)

    def notify_complete_dump(self, receivers=None):
        if receivers is None:
            receivers = []
        for notifier in self.notifiers:
            notifier.notify_complete_dump(receivers)

    def notify_failed_job(self, msg, stack_strace, receivers=None):
        if receivers is None:
            receivers = []
        for notifier in self.notifiers:
            notifier.notify_failed_job(msg, stack_strace, receivers)
