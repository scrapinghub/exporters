import unittest
from exporters.python_interpreter import Interpreter
from exporters.exceptions import InvalidExpression


class PythonExprInterpreter(unittest.TestCase):
    def setUp(self):
        self.interpreter = Interpreter()

    def test_eval_expression(self):
        self.assertEqual("oi", self.interpreter.eval("'o' + 'i'"))
        self.assertEqual([1, 2], self.interpreter.eval("[i for i in range(1, 3)]"))

    def test_eval_allow_simple_tuple_return(self):
        code = "1, 2"
        self.assertEquals((1, 2), self.interpreter.eval(code))

    def test_check_disallow_functions(self):
        code = "def f(): return []; f()"
        with self.assertRaises(InvalidExpression):
            self.assertEquals({}, self.interpreter.check(code))

    def test_check_disallow_fake_single_line_expr(self):
        code = "1; 2"
        with self.assertRaises(InvalidExpression):
            self.assertEquals({}, self.interpreter.check(code))

    def test_check_imports(self):
        code = "import datetime"
        with self.assertRaises(InvalidExpression):
            self.assertEquals({}, self.interpreter.check(code))
