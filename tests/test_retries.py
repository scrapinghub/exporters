import unittest
from exporters.default_retries import retry_short, disable_retries


class RetryTest(unittest.TestCase):
    def test__disable_retries(self):
        count_holder = [0]

        @retry_short
        def failing():
            count_holder[0] += 1
            raise RuntimeError('fail')

        def set_one_attempt(args, kwargs):
            kwargs.update(stop_max_attempt_number=0)
            return args, kwargs

        disable_retries()

        with self.assertRaises(RuntimeError):
            failing()

        self.assertEqual(count_holder[0], 1, "We should do only 1 attempt")
