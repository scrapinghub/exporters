from exporters.module_loader import ModuleLoader
from exporters.default_retries import retry_short


class NotifiersList(object):
    """
    This class is only used to support a list of notifications modules.
    """

    def __init__(self, options):
        self.options = options
        self.module_loader = ModuleLoader()
        self.notifiers = self._populate_notifiers()
        self.stats = {}

    def _populate_notifiers(self):
        notifiers_list = []
        for notifier in self.options:
            notifier_object = self.module_loader.load_notifier(notifier)
            notifiers_list.append(notifier_object)
        return notifiers_list

    def notify_start_dump(self, receivers=None, info=None):
        if receivers is None:
            receivers = []
        if info is None:
            info = {}
        for notifier in self.notifiers:
            notifier.notify_start_dump(receivers, info)

    def notify_complete_dump(self, receivers=None, info=None):
        if receivers is None:
            receivers = []
        if info is None:
            info = {}
        for notifier in self.notifiers:
            notifier.notify_complete_dump(receivers, info)

    def notify_failed_job(self, msg, stack_strace, receivers=None, info=None):
        if receivers is None:
            receivers = []
        if info is None:
            info = {}
        for notifier in self.notifiers:
            notifier.notify_failed_job(msg, stack_strace, receivers, info)
