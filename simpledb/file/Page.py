from simpledb.util.ByteBuffer import ByteBuffer
from simpledb.util.Integer import Integer


class Page:

    def __init__(self, *args):
        if isinstance(args[0], int):
            #  For creating data buffers
            blocksize = args[0]
            self.bb = ByteBuffer.allocateDirect(blocksize)
        elif isinstance(args[0], bytearray):
            #  For creating log pages
            b = args[0]
            self.bb = ByteBuffer.wrap(b)

    def getInt(self, offset):
        return self.bb.getInt(offset)

    def setInt(self, offset, n):
        self.bb.putInt(offset, n)

    def getBytes(self, offset):
        self.bb.position(offset)
        length = self.bb.getInt()
        b = bytearray(length)
        self.bb.get(b)
        return b

    def setBytes(self, offset, b):
        self.bb.position(offset)
        self.bb.putInt(len(b))
        self.bb.put(b)

    def getString(self, offset):
        b = self.getBytes(offset)
        return str(b, encoding=Integer.CHARSET)

    def setString(self, offset, s):
        b = bytearray(s, encoding=Integer.CHARSET)
        self.setBytes(offset, b)

    @staticmethod
    def maxLength(strlen):
        bytesPerChar = Integer.BYTESPERCHAR  # ANSI has 1 byte
        return Integer.BYTES + (strlen * bytesPerChar)

    #  a package private method, needed by FileMgr
    def contents(self):
        self.bb.position(0)
        return self.bb
