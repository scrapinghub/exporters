from exporters.pipeline.base_pipeline_item import BasePipelineItem


class BaseNotifier(BasePipelineItem):
    """
    This module takes care of notifications delivery.
    It has a slightly different architecture than the others due to the support
    of multiple notification endpoints to be loaded at the same time.
    As you can see in the provided example, the notifications
    parameter is an array of notification objects.
    To extend and add notification endpoints, they can implement the following methods:
    """

    def notify_start_dump(self, receivers=None):
        """
        Notifies the start of a dump to the receivers
        """
        raise NotImplementedError

    def notify_complete_dump(self, receivers=None):
        """
        Notifies the end of a dump to the receivers
        """
        raise NotImplementedError

    def notify_failed_job(self, mgs, stack_trace, receivers=None):
        """
        Notifies the failure of a dump to the receivers
        """
        raise NotImplementedError

    def set_metadata(self, key, value, module='notifier'):
        super(BaseNotifier, self).set_metadata(key, value, module)

    def update_metadata(self, data, module='notifier'):
        super(BaseNotifier, self).update_metadata(data, module)

    def get_metadata(self, key, module='notifier'):
        return super(BaseNotifier, self).get_metadata(key, module)

    def get_all_metadata(self, module='notifier'):
        return super(BaseNotifier, self).get_all_metadata(module)
