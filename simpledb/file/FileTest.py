from simpledb.file.BlockId import BlockId
from simpledb.file.FileMgr import FileMgr
from simpledb.file.Page import Page
from simpledb.util.File import File


class FileTest(object):
    @classmethod
    def main(cls, args):
        # db = SimpleDB("filetest", 400, 8)
        # fm = db.fileMgr()
        fm = FileMgr(File("filetest"), 400)
        blk = BlockId("testfile", 2)
        pos1 = 88
        p1 = Page(fm.blockSize())
        p1.setString(pos1, "abcdefghijklm")
        size = Page.maxLength(len("abcdefghijklm"))
        pos2 = pos1 + size
        p1.setInt(pos2, 345)
        fm.write(blk, p1)
        p2 = Page(fm.blockSize())
        fm.read(blk, p2)
        print("offset " + str(pos2) + " contains " + str(p2.getInt(pos2)))
        print("offset " + str(pos1) + " contains " + p2.getString(pos1))


if __name__ == '__main__':
    import sys
    FileTest.main(sys.argv)
