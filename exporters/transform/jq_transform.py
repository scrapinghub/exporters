import datetime
import json

import yaml
from exporters.records.base_record import BaseRecord
from exporters.transform.base_transform import BaseTransform


def default(o):
    if isinstance(o, datetime.datetime):
        return o.isoformat()
    return json.JSONEncoder.default(o)


class JQTransform(BaseTransform):
    """
    It applies jq transformations to items. To see documentation
    about possible jq transformations please refer to its
    `official documentation <http://stedolan.github.io/jq/manual/>`_.

        - jq_filter (str)
            Valid jq filter
    """
    supported_options = {
        'jq_filter': {'type': basestring}
    }

    def __init__(self, *args, **kwargs):
        super(JQTransform, self).__init__(*args, **kwargs)
        self.jq_expression = self.read_option('jq_filter')
        self.logger.info('JQTransform has been initiated. Expression: {}'.format(
            self.jq_expression))
        if not self.is_valid_jq_expression(self.jq_expression):
            raise ValueError('JQ expression is not valid')

    def _curate_item(self, item):
        return json.loads(json.dumps(item, default=default))

    def transform_batch(self, batch):
        from jq import jq
        jq_program = jq(self.jq_expression)
        for item in batch:
            try:
                item = self._curate_item(item)
                transformed_item = jq_program.transform(item)
            except StopIteration:
                # jq.transform() raise StopIteration for filtered items
                continue

            if not isinstance(transformed_item, dict):
                transformed_item = yaml.safe_load(transformed_item)

            yield BaseRecord(transformed_item)
        self.logger.debug('Transformed items')

    def is_valid_jq_expression(self, jq_expression):
        # TODO: Make a expression validator
        return True
