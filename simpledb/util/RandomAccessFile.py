from simpledb.util.FileChannel import FileChannel
from simpledb.util.Integer import Integer
import os
import struct


class RandomAccessFile:
    def __init__(self, file, mode):
        if os.path.exists(file.getname()) is False:
            open(file.getname(), "wb+")
        self.f = open(file.getname(), "rb+")
        self.filename = file.getname()

    def seek(self, offset):
        self.f.seek(offset)

    def readInt(self):
        x = struct.unpack('>i', self.f.read(Integer.BYTES))[0]
        return x

    def writeInt(self, x):
        self.f.write(struct.pack('>i', x))

    def read(self, byte_array):
        length = len(byte_array)
        byte_array[:] = self.f.read(length)

    def write(self, byte_array):
        self.f.write(byte_array)

    def close(self):
        self.f.close()

    def getChannel(self):
        return FileChannel(self.f)

    def length(self):
        return os.path.getsize(self.filename)


if __name__ == '__main__':
    from simpledb.util.File import *
    file = File("testfile")
    try:
        #  initialize the file
        f1 = RandomAccessFile(file, "rws")
        # f1.seek(123)
        # f1.writeInt(999)

        f1.seek(123)
        f2 = RandomAccessFile(file, "rws")
        # fileChannel = f1.getChannel()
        # from simpledb.file.Page import *
        # from simpledb.util.Integer import *
        # page = Page(Integer.BLOCKSIZE)
        # fileChannel.read(page.contents())
        # print(page.getInt(0))

        print(f1.readInt())
        print(f2.f.read())
        f1.close()
        f2.close()
    except IOError as e:
        print(e)
