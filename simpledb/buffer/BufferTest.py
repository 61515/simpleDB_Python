from simpledb.buffer.BufferMgr import BufferMgr
from simpledb.file.BlockId import BlockId
from simpledb.file.FileMgr import FileMgr
from simpledb.log.LogMgr import LogMgr
from simpledb.util.File import File


class BufferTest(object):
    @classmethod
    def main(cls, args):
        # db = SimpleDB("buffertest", 400, 3)  #  only 3 buffers
        # bm = db.bufferMgr()
        fm = FileMgr(File("buffertest"), 400)
        lm = LogMgr(fm, "logfile")
        bm = BufferMgr(fm, lm, 3)
        buff1 = bm.pin(BlockId("testfile", 1))
        p = buff1.contents()
        n = p.getInt(80)
        p.setInt(80, n + 1)
        buff1.setModified(1, 0)  # placeholder values
        print("The new value is " + str(n + 1))
        bm.unpin(buff1)
        #  One of these pins will flush buff1 to disk:
        buff2 = bm.pin(BlockId("testfile", 2))
        buff3 = bm.pin(BlockId("testfile", 3))
        buff4 = bm.pin(BlockId("testfile", 4))

        bm.unpin(buff2)
        buff2 = bm.pin(BlockId("testfile", 1))
        p2 = buff2.contents()
        p2.setInt(80, 9999)  # This modification
        buff2.setModified(1, 0)  # won't get written to disk.


if __name__ == '__main__':
    import sys

    BufferTest.main(sys.argv)
