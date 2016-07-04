import gzip
import os
import zipfile
from exporters.exceptions import UnsupportedCompressionFormat


class StreamZipFile(object):

    def __init__(self, path):
        self.path = path
        os.mknod(self.path)
        self.tmp_filename = path[:-4]
        self.tmp_file = open(self.tmp_filename, 'a')

    def write(self, content):
        self.tmp_file.write(content)

    def close(self):
        self.tmp_file.close()
        filename = os.path.basename(self.tmp_filename)
        with zipfile.ZipFile(self.path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            zip_file.write(self.tmp_filename, arcname=filename)


def get_compress_file(compression_format):
    if compression_format not in FILE_COMPRESSION:
        raise UnsupportedCompressionFormat(compression_format)
    return FILE_COMPRESSION[compression_format]


FILE_COMPRESSION = {
    'gz': lambda path: gzip.open(path, 'a'),
    'zip': StreamZipFile,
    'none': lambda path: open(path, 'a'),
}


try:
    from bz2file import BZ2File
except ImportError:
    import logging
    logging.info('Install bz2file to enable BZ2 compression.')
else:
    FILE_COMPRESSION['bz2'] = lambda path: BZ2File(path, 'a')
