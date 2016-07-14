def cohere_stream(stream):
    if isinstance(stream, IterIO):
        return stream
    return IterIO(stream)


def iterate_chunks(file, chunk_size):
    chunk = file.read(chunk_size)
    while chunk:
        yield chunk
        chunk = file.read(chunk_size)


class IterIO(object):
    """
    Both an iterator and a file-like object.

    Mode can be one one of:
    - chunks: iterator yields chunks that can be of various sizes. If iterator
              is a file-like object, chunks are of size chunk_size
    - lines:  iterator yields lines like standard file-like objects
    """
    def __init__(self, iterator, mode="chunks", chunk_size=1024):
        self._unconsumed = []
        self.mode = mode
        self._pos = 0
        if callable(getattr(iterator, 'read', None)):  # file-like object
            self._iterator = iterate_chunks(iterator, chunk_size)
        else:
            self._iterator = iterator
        self.finished = False
        self.closed = False

    def unshift(self, chunk):
        """
        Pushes a chunk of data back into the internal buffer. This is useful
        in certain situations where a stream is being consumed by code that
        needs to "un-consume" some amount of data that it has optimistically
        pulled out of the source, so that the data can be passed on to some
        other party.
        """
        if chunk:
            self._pos -= len(chunk)
            self._unconsumed.append(chunk)

    def __iter__(self):
        return self

    def next(self):
        if self.mode == 'chunks':
            return self.next_chunk()
        else:
            line = self.readline()
            if not line:
                raise StopIteration
            return line

    def next_chunk(self):
        if self._unconsumed:
            data = self._unconsumed.pop()
        else:
            data = self._iterator.next()  # Might raise StopIteration
        self._pos += len(data)
        return data

    def read(self, size=None):
        if size is None or size < 0:
            return "".join(list(self))
        else:
            data_chunks = []
            data_readed = 0
            try:
                while data_readed < size:
                    chunk = self.next_chunk()
                    data_chunks.append(chunk)
                    data_readed += len(chunk)
            except StopIteration:
                pass

            if data_readed > size:
                last_chunk = data_chunks.pop()
                extra_length = data_readed - size
                last_chunk, extra_data = last_chunk[:-extra_length], last_chunk[-extra_length:]
                self.unshift(extra_data)
                data_chunks.append(last_chunk)
            return "".join(data_chunks)

    def readline(self):
        line = ""
        n_pos = -1
        try:
            while n_pos < 0:
                line += self.next_chunk()
                n_pos = line.find('\n')
        except StopIteration:
            pass

        if n_pos >= 0:
            line, extra = line[:n_pos+1], line[n_pos+1:]
            self.unshift(extra)
        return line

    def iterlines(self):
        line = self.readline()
        while line:
            yield line
            line = self.readline()

    def readlines(self):
        return list(self.iterlines())

    def tell(self):
        return self._pos

    def close(self):
        if callable(getattr(self._iterator, 'close', None)):
            self._iterator.close()
        self._iterator = None
        self._unconsumed = None
        self.closed = True

    def seek(self, offset, from_what=0):
        if from_what == 0:  # From the begining
            if offset >= self.tell():
                self.seek(offset - self.tell(), from_what=1)
            else:
                raise NotImplementedError("Can't seek backwards")
        elif from_what == 1:  # From the cursor position
            if offset < 0:
                raise NotImplementedError("Can't seek backwards")
            else:
                self.read(offset)
        else:
            raise NotImplementedError("Can't seek from there")
