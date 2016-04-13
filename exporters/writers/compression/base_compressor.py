class SingleFileCompressor(object):
    extension = ''

    def compress(self, file_path):
        raise NotImplementedError

    def compressed_file_path(self, file_path):
        return file_path + self.extension

    def uncompressed_file_path(self, file_path):
        if file_path.endswith(self.extension):
            return file_path[:-len(self.extension)]
        return file_path
