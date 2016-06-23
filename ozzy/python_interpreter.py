import six
import ast

from .exceptions import InvalidExpression


def create_context(**kwargs):
    import datetime
    import re
    import itertools
    import calendar
    import math
    import random
    context = dict(kwargs)
    context.update(
        dict(
            datetime=datetime,
            re=re,
            itertools=itertools,
            calendar=calendar,
            math=math,
            random=random,
        ),
    )
    return context


class Interpreter(object):

    ast_allowed_nodes = (
        'keyword',
        'expr', 'name', 'load', 'call', 'store',
        'str', 'unicode', 'num', 'list', 'dict', 'set', 'tuple',  # Data types
        'unaryop', 'usub',  # Unary arithmetic operators
        # Binary arithmetic operators
        'binop', 'add', 'sub', 'div', 'mult', 'mod', 'pow', 'floordiv',
        'compare', 'eq', 'noteq', 'gt', 'lt', 'gte', 'lte',  # Comparison operators
        'bitand', 'bitor', 'bitxor', 'invert', 'lshift', 'rshift',  # Bitwise operators
        'boolop', 'and', 'or', 'not',  # Logical operators
        'in', 'notin',  # Membership operators
        'is', 'isnot',  # Identity operators
        'ifexp',  # Inline if statement
        'subscript', 'index', 'slice', 'extslice',  # Subscripting
        'listcomp', 'setcomp', 'dictcomp',  'generatorexp', 'comprehension',  # Comprehensions
        'attribute',  # Attribute access
    )

    allowed_objects = (
        str, unicode,  # strings
        int, float, long, complex,  # numbers
        list, dict, set, tuple,  # sequences
        type(None), bool  # others
    )

    def check(self, expression):
        if not isinstance(expression, six.string_types):
            raise InvalidExpression('Python expressions must be defined as strings')
        if not expression:
            raise InvalidExpression('Empty python expression')

        try:
            tree = ast.parse(expression)
        except SyntaxError as e:
            raise e

        if not tree.body:
            raise InvalidExpression('Empty python expression')
        elif len(tree.body) > 1:
            raise InvalidExpression('Python expressions must be a single line expression')

        start_node = tree.body[0]
        if not isinstance(start_node, ast.Expr):
            raise InvalidExpression("Python string must be an expression: '%s' found" %
                                    start_node.__class__.__name__)

        self._check_node(start_node)

    def eval(self, expression, context=None, check=True):
        if check:
            self.check(expression)
        return eval(expression, context)

    def _check_node(self, node):
        if isinstance(node, list):
            self._check_node_list(node)
        elif isinstance(node, ast.AST):
            if not self._is_allowed_ast_node(node):
                self._raise_not_allowed_node(node)
            self._check_node_fields(node)
        elif not isinstance(node, self.allowed_objects):
            self._raise_not_allowed_node(node)

    def _check_node_list(self, node_list):
        for node in node_list:
            self._check_node(node)

    def _check_node_fields(self, node):
        for field in [f for _, f in ast.iter_fields(node)]:
            self._check_node(field)

    def _is_allowed_ast_node(self, node):
        return node.__class__.__name__.lower() in self.ast_allowed_nodes

    def _raise_not_allowed_node(self, node):
        raise InvalidExpression("'%s' definition not allowed in python expressions" %
                                node.__class__.__name__)
