import yaml
from exporters.records.base_record import BaseRecord
from exporters.transform.base_transform import BaseTransform
from jq import jq


class JQTransform(BaseTransform):
    """
    It applies jq transformations to items. To see documentation about possible jq transformations please refer to its
    `official documentation <http://stedolan.github.io/jq/manual/>`_.

    Needed parameters:

        - jq_filter (str)
            Valid jq filter
    """
    # List of required options to set up the batch
    parameters = {
        'jq_filter': {'type': basestring}
    }

    def __init__(self, options, settings):
        super(JQTransform, self).__init__(options, settings)
        self.jq_expression = self.read_option('jq_filter')
        self.logger.info('JQTransform has been initiated. Expression: {}'.format(self.jq_expression))
        if not self.is_valid_jq_expression(self.jq_expression):
            raise ValueError('JQ expression is not valid')

    def transform_batch(self, batch):
        for item in batch:
            transformed_item = jq(self.jq_expression).transform(item)
            if not isinstance(transformed_item, dict):
                transformed_item = yaml.safe_load(transformed_item)
            new_base_record = BaseRecord()
            for key in transformed_item.keys():
                new_base_record[key] = transformed_item[key]
            yield new_base_record
        self.logger.debug('Transformed items')

    # TODO: Make a expression validator
    def is_valid_jq_expression(self, jq_expression):
        return True
