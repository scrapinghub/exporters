import os
import shutil
import tempfile
import uuid
from contextlib import contextmanager

import collections


# 50MB of chunk size for multipart uploads
import math

CHUNK_SIZE = 52428800


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


Chunk = collections.namedtuple('Chunk', 'bytes offset size number')


def split_file(file_path, chunk_size=CHUNK_SIZE):
    from filechunkio import FileChunkIO
    source_size = os.stat(file_path).st_size
    chunk_count = int(math.ceil(source_size / float(chunk_size)))
    for i in range(chunk_count):
        offset = chunk_size * i
        bytes = min(chunk_size, source_size - offset)
        with FileChunkIO(file_path, 'r', offset=offset, bytes=bytes) as fp:
            chunk = Chunk(fp, offset, chunk_size, i+1)
            yield chunk
