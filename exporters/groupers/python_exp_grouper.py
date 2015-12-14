from exporters.groupers.base_grouper import BaseGrouper
from exporters.python_interpreter import Interpreter, DEFAULT_CONTEXT


class PythonExpGrouper(BaseGrouper):
    """
    Groups items depending on python expressions. It adds the group membership information to items.

        - python_expressions (list)
            A list of python expressions to group by
    """
    supported_options = {
        'python_expressions': {'type': list}
    }

    def __init__(self, options):
        super(PythonExpGrouper, self).__init__(options)
        self.expressions = self.read_option('python_expressions', [])
        self.interpreter = Interpreter()

    def _get_membership(self, item):
        membership = []
        try:
            context = DEFAULT_CONTEXT.copy()
            context.update({'item': item})
            membership = [self.interpreter.eval(expression, context=context) for expression in self.expressions]
        except Exception as ex:
            self.logger.error(str(ex))
            raise
        return membership

    def group_batch(self, batch):
        for item in batch:
            item.group_membership = tuple(self._get_membership(item))
            yield item
