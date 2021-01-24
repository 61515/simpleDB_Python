from simpledb.buffer.BufferAbortException import BufferAbortException
from simpledb.buffer.BufferMgr import BufferMgr
from simpledb.file.BlockId import BlockId
from simpledb.log.LogMgr import LogMgr
from simpledb.util.File import File
from simpledb.file.FileMgr import FileMgr


class BufferMgrTest(object):
    @classmethod
    def main(cls, args):
        # db = SimpleDB("buffermgrtest", 400, 3)  #  only 3 buffers
        # bm = db.bufferMgr()

        fm = FileMgr(File("buffertest"), 400)
        lm = LogMgr(fm, "logfile")
        bm = BufferMgr(fm, lm, 3)

        buff = [None] * 6
        buff[0] = bm.pin(BlockId("testfile", 0))
        buff[1] = bm.pin(BlockId("testfile", 1))
        buff[2] = bm.pin(BlockId("testfile", 2))
        bm.unpin(buff[1])
        buff[1] = None
        buff[3] = bm.pin(BlockId("testfile", 0))  # block 0 pinned twice
        buff[4] = bm.pin(BlockId("testfile", 1))  # block 1 repinned
        print("Available buffers: " + str(bm.available()))
        try:
            print("Attempting to pin block 3...")
            buff[5] = bm.pin(BlockId("testfile", 3))  # will not work; no buffers left
        except BufferAbortException as e:
            print("Exception: No available buffers\n")

        bm.unpin(buff[2])
        buff[2] = None
        buff[5] = bm.pin(BlockId("testfile", 3))  # now this works
        print("Final Buffer Allocation:")

        for i in range(len(buff)):
            b = buff[i]
            if b is not None:
                print("buff[" + str(i) + "] pinned to block " + b.block().__str__())


if __name__ == '__main__':
    import sys

    BufferMgrTest.main(sys.argv)
