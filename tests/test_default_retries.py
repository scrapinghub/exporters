import unittest
from testfixtures import LogCapture
from exporters.default_retries import initialized_retry


class InitializeRetryTest(unittest.TestCase):

    def test_check_if_exceptions_are_being_warned(self):
        @initialized_retry(stop_max_attempt_number=3)
        def buggy():
            raise ValueError("oops")

        with LogCapture() as l:
            with self.assertRaisesRegexp(ValueError, "oops"):
                buggy()
        l.check(
            ('root', 'WARNING', 'Failed: buggy (message was: oops)'),
            ('root', 'WARNING', 'Failed: buggy (message was: oops)'),
            ('root', 'WARNING', 'Failed: buggy (message was: oops)'),
        )
