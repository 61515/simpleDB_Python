from simpledb.util.Integer import Integer


class FileChannel:
    def __init__(self, f):
        self.f = f
        blocksize = Integer.BLOCKSIZE

    def read(self, bytebuffer):
        length = len(bytebuffer.byte_array)
        read_bytes = bytearray(self.f.read(length))
        for i in range(len(read_bytes)):
            bytebuffer.byte_array[i] = read_bytes[i]

    def write(self, bytebuffer):
        self.f.write(bytebuffer.byte_array)
        self.f.flush()

    def close(self):
        self.f.close()
