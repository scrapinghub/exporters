import yaml
from exporters.records.base_record import BaseRecord
from exporters.transform.base_transform import BaseTransform


def _compile_jq(jq_expr):
    """Compile JQ expression, returning a JQ program object
    See: https://pypi.python.org/pypi/jq
    """
    import jq
    return jq.jq(jq_expr)


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
        self.jq_program = _compile_jq(self.jq_expression)

    def transform_batch(self, batch):
        for item in batch:
            try:
                transformed_item = self.jq_program.transform(item)
            except StopIteration:
                # jq.transform() raise StopIteration for filtered items
                continue

            if not isinstance(transformed_item, dict):
                transformed_item = yaml.safe_load(transformed_item)

            yield BaseRecord(transformed_item)
        self.logger.debug('Transformed items')
