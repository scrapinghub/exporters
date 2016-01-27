import os
import shutil
import tempfile
import uuid
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


@contextmanager
def TmpFile():
    tmp_folder = tempfile.mkdtemp()
    name = os.path.join(tmp_folder, str(uuid.uuid4()))
    try:
        yield name
    finally:
        shutil.rmtree(tmp_folder)
