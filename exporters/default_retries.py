import logging
import time
from contextlib import contextmanager
from decorator import decorator
from retrying import Retrying
from functools import wraps


__all__ = ['retry_short', 'retry_long', 'set_retry_init', 'disable_retries', 'retry_generator']


_retry_init = None


def set_retry_init(fn):
    global _retry_init
    _retry_init = fn


def _only_one_attempt(args, kwargs):
    kwargs = dict(kwargs, stop_max_attempt_number=0)
    return args, kwargs


def disable_retries():
    set_retry_init(_only_one_attempt)


def reenable_retries():
    set_retry_init(None)


@contextmanager
def disabled_retries():
    disable_retries()
    try:
        yield
    finally:
        reenable_retries()


@decorator
def _warn_about_exceptions(f, *args, **kw):
    try:
        return f(*args, **kw)
    except Exception as e:
        logging.warning("Retrying: {} (message was: {})".format(
            f.__name__, str(e)))
        raise


def initialized_retry(*dargs, **dkw):
    def wrap(f):
        @wraps(f)
        def wrapped_f(*args, **kw):
            if _retry_init:
                rargs, rkw = _retry_init(dargs, dkw)
            else:
                rargs, rkw = dargs, dkw
            return Retrying(*rargs, **rkw).call(_warn_about_exceptions(f), *args, **kw)

        return wrapped_f

    return wrap

# for operations that shouldn't take longer than a few seconds (e.g. HTTP request)
# will retry after 2s, 4s, 8s, 10s, 10s, 10s ... until the 10th attempt
retry_short = initialized_retry(
    wait_exponential_multiplier=1000,
    wait_exponential_max=10000,
    stop_max_attempt_number=10,
)

# for operations that take minutes to finish (e.g. uploading a file)
# will retry after 10s, 20s, 40s, 80s, 2m, ~5m, ~10m, ~20m and then give up.
retry_long = initialized_retry(
    wait_exponential_multiplier=5000,
    stop_max_attempt_number=8,
)


def retry_generator(fn, max_retries=8, retry_multiplier=5.0):
    """
    Retry a generator. The if you don't expect already yielded items to be
    yielded again, the generator will need to keep state about what items have
    already been successfully yielded.
    """
    @wraps(fn)
    def wrapper(*args, **kwargs):
        for retry in range(max_retries):
            try:
                generator = fn(*args, **kwargs)
                for i in generator:
                    yield i
            except StopIteration:
                raise
            except Exception as e:
                logging.warning("Retrying: {} (message was: {})".format(
                    fn.__name__, str(e)))
                time.sleep((retry+1)*retry_multiplier)
            else:
                break
    return wrapper
