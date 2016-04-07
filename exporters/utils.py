import os
import shutil
import tempfile
import uuid
from contextlib import contextmanager

import collections


def remove_if_exists(file_name):
    try:
        os.remove(file_name)
    except:
        pass


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


def nested_dict_value(d, path):
    final_value = d
    for k in path:
        if not isinstance(final_value, collections.Mapping):
            raise TypeError(
                'Could not get key {} from {} for item {} and value {}'.format(
                    k, final_value, d, path)
            )
        elif k in final_value:
            final_value = final_value[k]
        else:
            raise KeyError(
                '{} Key could not be found for nested path {} in {}'.format(k, path, d)
            )
    return final_value
