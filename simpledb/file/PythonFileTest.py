from simpledb.util.RandomAccessFile import RandomAccessFile
from simpledb.util.File import File
from simpledb.util.Integer import Integer


class PythonFileTest:
    @classmethod
    def main(cls, args):
        dbfile = File("testfile")
        f = RandomAccessFile(dbfile, "rws")
        pos1 = 88
        len1 = cls.writeString(f, pos1, "abcdefghijklm")
        pos2 = pos1 + len1
        cls.writeInt(f, pos2, 345)
        f.close()
        g = RandomAccessFile(dbfile, "rws")
        print("offset " + str(pos2) + " contains " + str(cls.readInt(g, pos2)))
        print("offset " + str(pos1) + " contains " + cls.readString(g, pos1))
        g.close()

    @staticmethod
    def readInt(f, pos):
        f.seek(pos)
        return f.readInt()

    @staticmethod
    def writeInt(f, pos, n):
        f.seek(pos)
        f.writeInt(n)
        return Integer.BYTES

    @staticmethod
    def readString(f, pos):
        f.seek(pos)
        length = f.readInt()
        byte_array = bytearray(length)
        f.read(byte_array)
        return bytearray.decode(byte_array, encoding=Integer.CHARSET)

    @staticmethod
    def writeString(f, pos, s):
        f.seek(pos)
        length = len(s)
        byte_array = str.encode(s, encoding=Integer.CHARSET)
        f.writeInt(length)
        f.write(byte_array)
        return Integer.BYTES + length


if __name__ == '__main__':
    import sys
    PythonFileTest.main(sys.argv)
