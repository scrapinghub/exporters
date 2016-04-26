import gzip
import os
from bz2file import BZ2File
import zipfile
from exporters.exceptions import UnsupportedCompressionFormat


class StreamCompressedFile(object):

    def append(self, content):
        self.file.write(content)

    def close(self):
        self.file.close()


class StreamGzipFile(StreamCompressedFile):

    def __init__(self, path):
        self.file = gzip.open(path, 'a')


class StreamZipFile(StreamCompressedFile):

    def __init__(self, path):
        self.path = path
        self.tmp_filename = path[:-4]
        self.tmp_file = open(self.tmp_filename, 'a')
        os.mknod(path)

    def append(self, content):
        self.tmp_file.write(content)

    def close(self):
        self.tmp_file.close()
        filename = os.path.basename(self.tmp_filename)
        with zipfile.ZipFile(self.path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            zip_file.write(self.tmp_filename, arcname=filename)


class StreamBz2File(StreamCompressedFile):

    def __init__(self, path):
        self.file = BZ2File(path, 'a')


class StreamNoCompressionFile(StreamCompressedFile):

    def __init__(self, path):
        self.file = open(path, 'a')


def get_compress_file(compression_format):
    if compression_format not in FILE_COMPRESSION:
        raise UnsupportedCompressionFormat(compression_format)
    return FILE_COMPRESSION[compression_format]


FILE_COMPRESSION = {
    'gz': StreamGzipFile,
    'zip': StreamZipFile,
    'bz2': StreamBz2File,
    'none': StreamNoCompressionFile,
}
