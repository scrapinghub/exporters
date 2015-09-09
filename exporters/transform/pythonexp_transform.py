from exporters.transform.base_transform import BaseTransform
from exporters.python_interpreter import Interpreter


class PythonexpTransform(BaseTransform):
    """
    It applies python expressions to items.

    Needed parameters:

        - python_expression (str)
            Valid python expression
    """
    # List of required options to set up the transform module
    requirements = {
        'python_expressions': {'type': list, 'required': True}
    }

    def __init__(self, options, settings):
        super(PythonexpTransform, self).__init__(options, settings)
        self.python_expressions = self.read_option('python_expressions')
        if not self.is_valid_python_expression(self.python_expressions):
            raise ValueError('Python expression is not valid')
        self.interpreter = Interpreter()
        self.logger.info('PythonexpTransform has been initiated. Expressions: {!r}'.format(self.python_expressions))

    def transform_batch(self, batch):
        for item in batch:
            for expression in self.python_expressions:
                self.interpreter.eval(expression, context={'item': item})
            yield item
        self.logger.debug('Transformed items')

    # TODO: Make a expression validator
    def is_valid_python_expression(self, python_expressions):
        return True
