import bz2
import gzip
import os
import shutil
import zipfile

from exporters.exceptions import UnsupportedCompressionFormat


def compress_bz2(file_path):
    bz2_file = file_path + '.bz2'
    with bz2.BZ2File(bz2_file, 'wb') as dump_file, open(file_path) as fl:
        shutil.copyfileobj(fl, dump_file)
    return bz2_file


def compress_gzip(file_path):
    gzipped_file = file_path + '.gz'
    with gzip.open(gzipped_file, 'wb') as dump_file, open(file_path) as fl:
        shutil.copyfileobj(fl, dump_file)
    return gzipped_file


def compress_zip(file_path):
    _, filename = os.path.split(file_path)
    zipped_file_path = file_path + '.zip'
    with zipfile.ZipFile(zipped_file_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.write(file_path, arcname=filename)
    return zipped_file_path


def uncompressed_file_path(compression_file_path):
    file_path, compression_extension = os.path.splitext(compression_file_path)
    return file_path


def validate_compression_format(compression_format):
    if compression_format not in FILE_COMPRESSION:
        raise UnsupportedCompressionFormat


def compress(file_path, compression_format):
    validate_compression_format(compression_format)
    return FILE_COMPRESSION[compression_format](file_path)

FILE_COMPRESSION = {
    'gz': compress_gzip,
    'zip': compress_zip,
    'bz2': compress_bz2,
}
