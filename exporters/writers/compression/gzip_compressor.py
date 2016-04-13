import gzip
import shutil

from exporters.writers.compression.base_compressor import SingleFileCompressor


class GzipCompressor(SingleFileCompressor):
    extension = '.gz'

    def compress(self, file_path):
        gzipped_file = self.compressed_file_path(file_path)
        with gzip.open(gzipped_file, 'wb') as dump_file, open(file_path) as fl:
            shutil.copyfileobj(fl, dump_file)
        return gzipped_file
