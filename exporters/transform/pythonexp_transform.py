from exporters.transform.base_transform import BaseTransform
from exporters.python_interpreter import Interpreter, create_context
from exporters.exceptions import InvalidExpression


class PythonexpTransform(BaseTransform):
    """
    It applies python expressions to items.

        - python_expression (str)
            Valid python expression
    """
    # List of options to set up the transform module
    supported_options = {
        'python_expressions': {'type': list}
    }

    def __init__(self, options):
        super(PythonexpTransform, self).__init__(options)
        self.python_expressions = self.read_option('python_expressions')
        self.interpreter = Interpreter()
        for expr in self.python_expressions:
            self.interpreter.check(expr)
        self.logger.info('PythonexpTransform has been initiated. Expressions: {!r}'.format(self.python_expressions))

    def transform_batch(self, batch):
        for item in batch:
            context = create_context()
            context.update({'item': item})
            for expression in self.python_expressions:
                self.interpreter.eval(expression, context=context)
            yield item
        self.logger.debug('Transformed items')
