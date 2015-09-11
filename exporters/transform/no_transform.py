from exporters.transform.base_transform import BaseTransform


class NoTransform(BaseTransform):
    """
    It leaves the batch as is. This is provided for the cases where no transformations are needed on the original items.
    """
    # List of required options to set up the batch
    parameters = {}

    def __init__(self, options, settings):
        super(NoTransform, self).__init__(options, settings)
        self.logger.info('NoTransform has been initiated')

    def transform_batch(self, batch):
        self.logger.debug('Transformed items')
        return batch
