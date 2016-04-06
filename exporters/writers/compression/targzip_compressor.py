import os
import tarfile

from exporters.writers.compression.base_compressor import SingleFileCompressor


class TarGzipCompressor(SingleFileCompressor):
    extension = '.tar.gz'

    def compress(self, file_path):
        _, filename = os.path.split(file_path)
        tar_filepath = file_path + self.extension
        with tarfile.open(tar_filepath, 'w:gz') as tar_file:
            tar_file.add(file_path, arcname=filename)
        return tar_filepath
