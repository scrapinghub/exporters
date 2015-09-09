from exporters.filters.base_filter import BaseFilter
from exporters.python_interpreter import Interpreter


class PythonexpFilter(BaseFilter):
    """
    Filter items depending on python expression.

    Needed parameters:

        - python_expression (str)
            Python expression to filter by.
    """
    # List of required options
    requirements = {
        'python_expression': {'type': basestring, 'required': True}
    }

    def __init__(self, options, settings):
        super(PythonexpFilter, self).__init__(options, settings)
        self.expression = self.read_option('python_expression')
        self.interpreter = Interpreter()
        self.logger.info('PythonexpFilter has been initiated. Expression: {!r}'.format(self.expression))

    def filter(self, item):
        try:
            return self.interpreter.eval(self.expression, context={'item': item})
        except Exception as ex:
            self.logger.error(str(ex))
            raise
