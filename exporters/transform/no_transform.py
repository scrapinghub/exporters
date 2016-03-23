from exporters.transform.base_transform import BaseTransform


class NoTransform(BaseTransform):
    """
    It leaves the batch as is.
    This is provided for the cases where no transformations are needed on the original items.
    """

    def __init__(self, *args, **kwargs):
        super(NoTransform, self).__init__(*args, **kwargs)

    def transform_batch(self, batch):
        return batch
