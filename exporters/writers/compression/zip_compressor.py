import os
import zipfile

from exporters.writers.compression.base_compressor import SingleFileCompressor


class ZipCompressor(SingleFileCompressor):
    extension = '.zip'

    def compress(self, file_path):
        _, filename = os.path.split(file_path)
        zipped_file_path = file_path + self.extension
        with zipfile.ZipFile(zipped_file_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            zip_file.write(file_path, arcname=filename)
        return zipped_file_path
