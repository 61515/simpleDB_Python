from simpledb.buffer.BufferMgr import BufferMgr
from simpledb.file.BlockId import BlockId
from simpledb.file.Page import Page
from simpledb.log.LogMgr import LogMgr
from simpledb.tx.Transaction import Transaction
from simpledb.util.File import File
from simpledb.file.FileMgr import FileMgr
from simpledb.util.Integer import Integer


class RecoveryTest(object):

    fm = None
    lm = None
    bm = None
    blk0 = None
    blk1 = None

    @classmethod
    def main(cls, args):
        # db = SimpleDB("recoverytest", 400, 8)
        cls.fm = FileMgr(File("recoverytest"), 400)
        cls.lm = LogMgr(cls.fm, "simpledb.log")
        cls.bm = BufferMgr(cls.fm, cls.lm, 8)

        cls.blk0 = BlockId("testfile", 0)
        cls.blk1 = BlockId("testfile", 1)

        if cls.fm.length("testfile") == 0:
            cls.initialize()
            cls.modify()
        else:
            cls.recover()

    @staticmethod
    def initialize():
        tx1 = Transaction(RecoveryTest.fm, RecoveryTest.lm, RecoveryTest.bm)
        tx2 = Transaction(RecoveryTest.fm, RecoveryTest.lm, RecoveryTest.bm)
        tx1.pin(RecoveryTest.blk0)
        tx2.pin(RecoveryTest.blk1)
        pos = 0

        for i in range(6):
            tx1.setInt(RecoveryTest.blk0, pos, pos, False)
            tx2.setInt(RecoveryTest.blk1, pos, pos, False)
            pos += Integer.BYTES

        tx1.setString(RecoveryTest.blk0, 30, "abc", False)
        tx2.setString(RecoveryTest.blk1, 30, "def", False)
        tx1.commit()
        tx2.commit()
        RecoveryTest.printValues("After Initialization:")

    @staticmethod
    def modify():
        tx3 = Transaction(RecoveryTest.fm, RecoveryTest.lm, RecoveryTest.bm)
        tx4 = Transaction(RecoveryTest.fm, RecoveryTest.lm, RecoveryTest.bm)
        tx3.pin(RecoveryTest.blk0)
        tx4.pin(RecoveryTest.blk1)
        pos = 0

        for i in range(6):
            tx3.setInt(RecoveryTest.blk0, pos, pos + 100, True)
            tx4.setInt(RecoveryTest.blk1, pos, pos + 100, True)
            pos += Integer.BYTES

        tx3.setString(RecoveryTest.blk0, 30, "uvw", True)
        tx4.setString(RecoveryTest.blk1, 30, "xyz", True)
        RecoveryTest.bm.flushAll(3)
        RecoveryTest.bm.flushAll(4)
        RecoveryTest.printValues("After modification:")

        tx3.rollback()
        RecoveryTest.printValues("After rollback:")
        #  tx4 stops here without committing or rolling back,
        #  so all its changes should be undone during recovery.

    @staticmethod
    def recover():
        tx = Transaction(RecoveryTest.fm, RecoveryTest.lm, RecoveryTest.bm)
        tx.recover()
        RecoveryTest.printValues("After recovery:")

    #  Print the values that made it to disk.
    @staticmethod
    def printValues(msg):
        print(msg)
        p0 = Page(RecoveryTest.fm.blockSize())
        p1 = Page(RecoveryTest.fm.blockSize())
        RecoveryTest.fm.read(RecoveryTest.blk0, p0)
        RecoveryTest.fm.read(RecoveryTest.blk1, p1)
        pos = 0

        for i in range(6):
            print(str(p0.getInt(pos)) + " ", end='')
            print(str(p1.getInt(pos)) + " ", end='')
            pos += Integer.BYTES

        print(p0.getString(30) + " ", end='')
        print(p1.getString(30) + " ", end='')
        print()


if __name__ == '__main__':
    import sys
    RecoveryTest.main(sys.argv)
