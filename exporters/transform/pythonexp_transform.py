from exporters.transform.base_transform import BaseTransform
from exporters.python_interpreter import Interpreter, create_context
from exporters.utils import str_list


class PythonexpTransform(BaseTransform):
    """
    It applies python expressions to items.

        - python_expression (str)
            Valid python expression
    """
    # List of options to set up the transform module
    supported_options = {
        'python_expressions': {'type': str_list}
    }

    def __init__(self, *args, **kwargs):
        super(PythonexpTransform, self).__init__(*args, **kwargs)
        self.python_expressions = self.read_option('python_expressions')
        if not self.is_valid_python_expression(self.python_expressions):
            raise ValueError('Python expression is not valid')
        self.interpreter = Interpreter()
        self.logger.info('PythonexpTransform has been initiated. Expressions: {!r}'.format(
            self.python_expressions)
        )

    def transform_batch(self, batch):
        for item in batch:
            context = create_context(item=item)
            for expression in self.python_expressions:
                self.interpreter.eval(expression, context=context)
            yield item
        self.logger.debug('Transformed items')

    # TODO: Make a expression validator
    def is_valid_python_expression(self, python_expressions):
        return True
