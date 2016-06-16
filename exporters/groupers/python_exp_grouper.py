from exporters.groupers.base_grouper import BaseGrouper
from exporters.python_interpreter import Interpreter, create_context
from exporters.utils import str_list


class PythonExpGrouper(BaseGrouper):
    """
    Groups items depending on python expressions. It adds the group membership information to items.

        - python_expressions (list)
            A list of python expressions to group by
    """
    supported_options = {
        'python_expressions': {'type': str_list}
    }

    def __init__(self, *args, **kwargs):
        super(PythonExpGrouper, self).__init__(*args, **kwargs)
        self.expressions = self.read_option('python_expressions', [])
        self.interpreter = Interpreter()

    def _get_membership(self, item):
        try:
            context = create_context(item=item)
            return [
                self.interpreter.eval(expression, context=context)
                for expression in self.expressions
            ]
        except Exception as ex:
            self.logger.error(str(ex))
            raise

    def group_batch(self, batch):
        for item in batch:
            item.group_membership = tuple(self._get_membership(item))
            yield item
