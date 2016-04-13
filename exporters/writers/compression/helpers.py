from exporters.exceptions import InvalidCompressionFormat
from exporters.writers.compression.bzip2_compressor import Bzip2Compressor
from exporters.writers.compression.gzip_compressor import GzipCompressor
from exporters.writers.compression.targzip_compressor import TarGzipCompressor
from exporters.writers.compression.zip_compressor import ZipCompressor

SUPPORTED_COMPRESSORS = {'gzip': GzipCompressor,
                         'zip': ZipCompressor,
                         'bz2': Bzip2Compressor,
                         'tgz': TarGzipCompressor}


def get_file_compressor(compression_format):
    compressor = SUPPORTED_COMPRESSORS.get(compression_format.lower())
    if not compressor:
        raise InvalidCompressionFormat
    return compressor()
