from exporters.filters.base_filter import BaseFilter
from exporters.python_interpreter import Interpreter, create_context
from importlib import import_module


def load_imports(imports):
    # XXX: should we blacklist (or whitelist?) imports here?
    return {name: import_module(mod) for name, mod in imports.items()}


class PythonexpFilter(BaseFilter):
    """
    Filter items depending on python expression. This is NOT sure, so make sure you only use
    it in contained environments

        - python_expression (str)
            Python expression to filter by

        - imports(dict)
            An object with neede imports for expressions
    """
    # List of options
    supported_options = {
        'python_expression': {'type': basestring},
        'imports': {'type': dict, 'default': {}},
    }

    def __init__(self, *args, **kwargs):
        super(PythonexpFilter, self).__init__(*args, **kwargs)
        self.logger.warning('PythonexpFilter can import insecure code'
                            ' -- only use it in contained environments')
        self.expression = self.read_option('python_expression')
        self.imports = load_imports(self.read_option('imports'))
        self.interpreter = Interpreter()
        self.logger.info('PythonexpFilter has been initiated.'
                         ' Expression: {!r}'.format(self.expression))

    def filter(self, item):
        try:
            context = create_context(item=item, **self.imports)
            return self.interpreter.eval(self.expression, context=context)
        except Exception as ex:
            self.logger.error(str(ex))
            raise
