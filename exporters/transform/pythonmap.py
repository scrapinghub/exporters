from exporters.transform.base_transform import BaseTransform
from exporters.python_interpreter import Interpreter, create_context


class PythonMapTransform(BaseTransform):
    """Transform implementation that maps items using Python expressions
    """
    supported_options = {
        "map": {'type': basestring},
    }

    def __init__(self, *args, **kwargs):
        super(PythonMapTransform, self).__init__(*args, **kwargs)
        self.map_expression = self.read_option('map')
        self.interpreter = Interpreter()
        self.interpreter.check(self.map_expression)

    def _map_item(self, it):
        context = create_context(item=it)
        return self.interpreter.eval(expression=self.map_expression, context=context)

    def transform_batch(self, batch):
        return (self._map_item(it) for it in batch)
