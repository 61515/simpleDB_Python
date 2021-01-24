from simpledb.util.Integer import Integer
import struct


class ByteBuffer:
    def __init__(self, bytesize):
        self.pos = 0
        self.limit = bytesize
        self.capacity = bytesize
        self.byte_array = None
        # self.byte_array = bytearray(bytesize)

    @staticmethod
    def allocateDirect(bytesize):
        bytebuffer = ByteBuffer(bytesize)
        # The source code is the memory allocation IO on the operating system
        bytebuffer.byte_array = bytearray(bytesize)
        return bytebuffer

    @staticmethod
    def wrap(byte_array):
        bytesize = len(byte_array)
        bytebuffer = ByteBuffer(bytesize)

        # for i in range(len(byte_array)):
        #     bytebuffer.byte_array[i] = byte_array[i]
        bytebuffer.byte_array = byte_array
        return bytebuffer

    def position(self, newPos):
        self.pos = newPos

    def putInt(self, *args):
        if len(args) == 1:
            val = args[0]
            self.byte_array[self.pos: self.pos + Integer.BYTES] = struct.pack('>i', val)
            self.pos += Integer.BYTES
        elif len(args) == 2:
            offset, val = args
            self.byte_array[offset: offset + Integer.BYTES] = struct.pack('>i', val)

    def getInt(self, *args):
        if len(args) == 0:
            x = struct.unpack('>i', self.byte_array[self.pos: self.pos + Integer.BYTES])[0]
            self.pos += Integer.BYTES
            return x
        elif len(args) == 1:
            offset = args[0]
            x = struct.unpack('>i', self.byte_array[offset: offset + Integer.BYTES])[0]
            return x

    def put(self, byte_array):
        length = len(byte_array)
        self.byte_array[self.pos: self.pos + length] = byte_array

    def get(self, byte_array):
        length = len(byte_array)
        byte_array[:] = self.byte_array[self.pos: self.pos + length]


# if __name__ == '__main__':
#     bb = ByteBuffer.allocateDirect(16)
#     bb.putInt(33)
#     bb.position(0)
#     print(bb.getInt())
