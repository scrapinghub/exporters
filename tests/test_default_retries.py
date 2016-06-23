import unittest

from testfixtures import LogCapture

from ozzy.default_retries import (disable_retries, disabled_retries,
                                  initialized_retry, reenable_retries,
                                  retry_short)


class InitializeRetryTest(unittest.TestCase):

    def test_check_if_exceptions_are_being_warned(self):
        @initialized_retry(stop_max_attempt_number=3)
        def buggy():
            raise RuntimeError("oops")

        with LogCapture() as l:
            with self.assertRaisesRegexp(RuntimeError, "oops"):
                buggy()
        l.check(
            ('root', 'WARNING', 'Failed: buggy (message was: oops)'),
            ('root', 'WARNING', 'Failed: buggy (message was: oops)'),
            ('root', 'WARNING', 'Failed: buggy (message was: oops)'),
        )

    def test_by_default_retries_are_enabled(self):
        # given:
        count_holder = [0]

        @initialized_retry(stop_max_attempt_number=4)
        def buggy():
            count_holder[0] += 1
            raise RuntimeError("oops")

        # when:
        with self.assertRaisesRegexp(RuntimeError, "oops"):
            buggy()

        # then:
        self.assertEqual(4, count_holder[0])

    def test_should_disable_retries(self):
        count_holder = [0]

        @retry_short
        def buggy():
            count_holder[0] += 1
            raise RuntimeError('oops')

        disable_retries()

        with self.assertRaisesRegexp(RuntimeError, "oops"):
            buggy()

        self.assertEqual(count_holder[0], 1, "We should do only 1 attempt")

    def test_context_manager_should_disable_and_reenable_retries(self):
        # given
        count_holder = [0]

        @initialized_retry(stop_max_attempt_number=4)
        def buggy():
            count_holder[0] += 1
            raise RuntimeError("oops")

        # when:
        with disabled_retries():
            with self.assertRaisesRegexp(RuntimeError, "oops"):
                buggy()

        # then:
        self.assertEqual(1, count_holder[0])

        # and when:
        reenable_retries()
        with self.assertRaisesRegexp(RuntimeError, "oops"):
            buggy()

        # then
        self.assertEqual(5, count_holder[0])
