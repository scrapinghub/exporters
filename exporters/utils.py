import os
import shutil
import tempfile
from contextlib import contextmanager


def remove_if_exists(file_name):
    try: os.remove(file_name)
    except: pass


@contextmanager
def TemporaryDirectory():
    name = tempfile.mkdtemp()
    try:
        yield name
    finally:
        shutil.rmtree(name)
