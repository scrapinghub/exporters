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


def get_substrings(start, end, s):
    # Gets a list of substrings between two delimiters
    words = []
    partial = ''
    for letter in s:
        if letter == start:
            partial = ''
        elif letter == end:
            words.append(partial)
            partial = ''
        else:
            partial += letter
    return words