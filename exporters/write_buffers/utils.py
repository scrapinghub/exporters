import hashlib


def get_filename(name_without_ext, file_extension, compression_format):
    if compression_format != 'none':
        return '{}.{}.{}'.format(name_without_ext, file_extension, compression_format)
    else:
        return '{}.{}'.format(name_without_ext, file_extension)


def hash_for_file(path, algorithm, block_size=256*128):
    hash = hashlib.new(algorithm)
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(block_size), b''):
            hash.update(chunk)
    return hash.hexdigest()
