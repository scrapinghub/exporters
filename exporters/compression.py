import bz2
import gzip
import os
import shutil
import zipfile

from exporters.exceptions import UnsupportedCompressionFormat


def compress_bz2(dump_file, source_file):
    with bz2.BZ2File(dump_file.name, 'wb') as bz_file, open(source_file.name) as fl:
        shutil.copyfileobj(fl, bz_file)


def compress_gzip(dump_file, source_file):
    with gzip.GzipFile(fileobj=dump_file, mode='wb') as gz_file:
        shutil.copyfileobj(source_file, gz_file)


def compress_zip(dump_file, source_file):
    filename = os.path.basename(source_file.name)
    with zipfile.ZipFile(dump_file, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.write(source_file.name, arcname=filename)


def get_compress_func(compression_format):
    if compression_format not in FILE_COMPRESSION:
        raise UnsupportedCompressionFormat(compression_format)
    return FILE_COMPRESSION[compression_format]

FILE_COMPRESSION = {
    'gz': compress_gzip,
    'zip': compress_zip,
    'bz2': compress_bz2,
}
