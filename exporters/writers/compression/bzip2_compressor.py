import bz2
import shutil

from exporters.writers.compression.base_compressor import SingleFileCompressor


class Bzip2Compressor(SingleFileCompressor):
    extension = '.bz2'

    def compress(self, file_path):
        bz2_file = self.compressed_file_path(file_path)
        with bz2.BZ2File(bz2_file, 'wb') as dump_file, open(file_path) as fl:
            shutil.copyfileobj(fl, dump_file)
        return bz2_file
