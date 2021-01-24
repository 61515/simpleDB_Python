from simpledb.util.File import File
from simpledb.util.RandomAccessFile import RandomAccessFile


class RandomAccessFileTest(object):
    @classmethod
    def main(cls, args):
        file = File("testfile")
        try:
            #  initialize the file
            f1 = RandomAccessFile(file, "rws")
            f1.seek(123)
            f1.writeInt(999)
            f1.close()
            #  increment the file
            f2 = RandomAccessFile(file, "rws")
            f2.seek(123)
            n = f2.readInt()
            f2.seek(123)
            f2.writeInt(n + 1)
            f2.close()
            #  re-read the file
            f3 = RandomAccessFile(file, "rws")
            f3.seek(123)
            print("The new value is " + str(f3.readInt()))
            f3.close()
        except IOError as e:
            print(e)


if __name__ == '__main__':
    import sys
    RandomAccessFileTest.main(sys.argv)
