import json
import gzip
from exporters.default_retries import retry_long

from exporters.writers.base_writer import BaseWriter


class ODOWriter(BaseWriter):
    """
    Writes items to a odo destination. https://odo.readthedocs.org/en/latest/

    Needed parameters:

        - schema (object)
            schema object.

        - odo_uri (str)
            ODO valid destination uri.
    """

    requirements = {
        'schema': {'type': object, 'required': True},
        'odo_uri': {'type': basestring, 'required': True}
    }

    def __init__(self, options):
        super(ODOWriter, self).__init__(options)
        from flatson import Flatson
        schema = self.read_option('schema', None)
        self.odo_uri = self.read_option('odo_uri', None)
        self.flatson = Flatson(schema)
        self.logger.info('ODOWriter has been initiated. Writing to: {}'.format(self.odo_uri))

    @retry_long
    def write(self, dump_path, group_key=''):
        from odo import odo, resource, discover
        import pandas as pd
        with gzip.open(dump_path) as f:
            lines = [json.loads(line.replace('\n', '')) for line in f.readlines()]
        flattened_lines = (self.flatson.flatten(line) for line in lines)
        pf = pd.DataFrame(flattened_lines, columns=self.flatson.fieldnames)
        dshape = discover(pf)
        odo(pf, resource(self.odo_uri), dshape=dshape)
