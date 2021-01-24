from simpledb.buffer.BufferMgr import BufferMgr
from simpledb.file.BlockId import BlockId
from simpledb.log.LogMgr import LogMgr
from simpledb.tx.Transaction import Transaction
from simpledb.util.File import File
from simpledb.file.FileMgr import FileMgr
import threading
import time


class ConcurrencyTest(object):
    fm = None
    lm = None
    bm = None

    @classmethod
    def main(cls, args):
        # initialize the database system
        # db = SimpleDB("concurrencytest", 400, 8)
        cls.fm = FileMgr(File("recoverytest"), 400)
        cls.lm = LogMgr(cls.fm, "simpledb.log")
        cls.bm = BufferMgr(cls.fm, cls.lm, 8)

        a = cls.A()
        a.start()
        b = cls.B()
        b.start()
        c = cls.C()
        c.start()

    class A(threading.Thread):
        def run(self):
            try:
                txA = Transaction(ConcurrencyTest.fm, ConcurrencyTest.lm, ConcurrencyTest.bm)
                blk1 = BlockId("testfile", 1)
                blk2 = BlockId("testfile", 2)
                txA.pin(blk1)
                txA.pin(blk2)
                print("Tx A: request slock 1")
                txA.getInt(blk1, 0)
                print("Tx A: receive slock 1")
                time.sleep(1)

                print("Tx A: request slock 2")
                txA.getInt(blk2, 0)
                print("Tx A: receive slock 2")
                txA.commit()
                print("Tx A: commit")
            except InterruptedError as e:
                pass

    class B(threading.Thread):
        def run(self):
            try:
                txB = Transaction(ConcurrencyTest.fm, ConcurrencyTest.lm, ConcurrencyTest.bm)
                blk1 = BlockId("testfile", 1)
                blk2 = BlockId("testfile", 2)
                txB.pin(blk1)
                txB.pin(blk2)
                print("Tx B: request xlock 2")
                txB.setInt(blk2, 0, 0, False)
                print("Tx B: receive xlock 2")
                time.sleep(1)

                print("Tx B: request slock 1")
                txB.getInt(blk1, 0)
                print("Tx B: receive slock 1")
                txB.commit()
                print("Tx B: commit")
            except InterruptedError as e:
                pass

    class C(threading.Thread):
        def run(self):
            try:
                txC = Transaction(ConcurrencyTest.fm, ConcurrencyTest.lm, ConcurrencyTest.bm)
                blk1 = BlockId("testfile", 1)
                blk2 = BlockId("testfile", 2)
                txC.pin(blk1)
                txC.pin(blk2)
                time.sleep(0.5)

                print("Tx C: request xlock 1")
                txC.setInt(blk1, 0, 0, False)
                print("Tx C: receive xlock 1")
                time.sleep(1)

                print("Tx C: request slock 2")
                txC.getInt(blk2, 0)
                print("Tx C: receive slock 2")
                txC.commit()
                print("Tx C: commit")
            except InterruptedError as e:
                pass


if __name__ == '__main__':
    import sys
    ConcurrencyTest.main(sys.argv)
