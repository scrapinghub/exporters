from exporters.transform.base_transform import BaseTransform


class NoTransform(BaseTransform):
    """
    It leaves the batch as is.
    This is provided for the cases where no transformations are needed on the original items.
    """

    def __init__(self, options):
        super(NoTransform, self).__init__(options)

    def transform_batch(self, batch):
        return batch
