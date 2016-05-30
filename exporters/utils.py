import logging
import os
import shutil
import tempfile
import uuid
from contextlib import contextmanager
import collections

# 50MB of chunk size for multipart uploads
import re

CHUNK_SIZE = 50 * 1024 * 1024


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
    chunk_number = 0
    while True:
        offset = chunk_size * chunk_number
        if offset >= source_size:
            break
        bytes = min(chunk_size, source_size - offset)
        fp = FileChunkIO(file_path, 'r', offset=offset, bytes=bytes)
        chunk = Chunk(fp, offset, bytes, chunk_number+1)
        yield chunk
        chunk_number += 1


def calculate_multipart_etag(source_path, chunk_size):
    import hashlib
    md5s = []

    with open(source_path, 'rb') as fp:
        while True:
            data = fp.read(chunk_size)
            if not data:
                break
            md5s.append(hashlib.md5(data))

    digests = b"".join(m.digest() for m in md5s)
    new_md5 = hashlib.md5(digests)
    new_etag = '"%s-%s"' % (new_md5.hexdigest(), len(md5s))
    return new_etag


def read_option(option_name, options, supported_options, default=None):
    if option_name in options:
        return options.get(option_name)
    env_name = supported_options.get(option_name, {}).get('env_fallback')
    if env_name and env_name in os.environ:
        return os.environ.get(env_name)
    if env_name:
        logging.log(logging.WARNING, 'Missing value for option {}. (tried also: {} from env)'
                    .format(option_name, env_name))
    return supported_options.get(option_name, {}).get('default', default)


BUCKET_RE = '(s3:\/\/|)([a-zA-Z\.0-9_\-]*)(\/|)'


def get_bucket_name(bucket):
    groups = re.match(BUCKET_RE, bucket).groups()
    if len(groups) > 1:
        bucket = groups[1]
    return bucket
