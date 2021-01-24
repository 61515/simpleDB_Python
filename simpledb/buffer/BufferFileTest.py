from simpledb.buffer.BufferMgr import BufferMgr
from simpledb.file.BlockId import BlockId
from simpledb.file.Page import Page
from simpledb.log.LogMgr import LogMgr
from simpledb.util.File import File
from simpledb.file.FileMgr import FileMgr


class BufferFileTest(object):
    @classmethod
    def main(cls, args):
        # db = SimpleDB("bufferfiletest", 400, 8)
        # bm = db.bufferMgr()
        fm = FileMgr(File("buffertest"), 400)
        lm = LogMgr(fm, "logfile")
        bm = BufferMgr(fm, lm, 8)
        blk = BlockId("testfile", 2)
        pos1 = 88
        b1 = bm.pin(blk)
        p1 = b1.contents()
        p1.setString(pos1, "abcdefghijklm")
        size = Page.maxLength(len("abcdefghijklm"))
        pos2 = pos1 + size
        p1.setInt(pos2, 345)
        b1.setModified(1, 0)
        bm.unpin(b1)
        b2 = bm.pin(blk)
        p2 = b2.contents()
        print("offset " + str(pos2) + " contains " + str(p2.getInt(pos2)))
        print("offset " + str(pos1) + " contains " + p2.getString(pos1))
        bm.unpin(b2)


if __name__ == '__main__':
    import sys
    BufferFileTest.main(sys.argv)
