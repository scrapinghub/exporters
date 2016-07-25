from exporters.transform.base_transform import BaseTransform
from flatson import Flatson


class FlatsonTransform(BaseTransform):
    """
    It flatten a JSON-like dataset into flat CSV-like tables using the
    Flatson library, please refer to Flatson
    `official documentation
    <http://flatson.readthedocs.io/en/latest/readme.html>`_.

        - flatson_schema (dict)
            Valid Flatson schema
    """
    # List of options to set up the transform module
    supported_options = {
        'flatson_schema': {'type': dict}
    }

    def __init__(self, *args, **kwargs):
        super(FlatsonTransform, self).__init__(*args, **kwargs)
        self.flatson_schema = self.read_option('flatson_schema')
        if not self.is_valid_schema(self.flatson_schema):
            raise ValueError('Invalid flatson schema')
        self.logger.info(
            'FlatsonTransform has been initiated. Schema: {!r}'.format(
                self.flatson_schema))

    def is_valid_schema(self, schema):
        try:
            self.flatson = Flatson(schema)
            return True
        except ValueError:
            return False

    def transform_batch(self, batch):
        for record in batch:
            yield self.flatson.flatten(record)
