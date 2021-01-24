from simpledb.buffer.BufferMgr import BufferMgr
from simpledb.file.BlockId import BlockId
from simpledb.log.LogMgr import LogMgr
from simpledb.tx.Transaction import Transaction
from simpledb.util.File import File
from simpledb.file.FileMgr import FileMgr


class TxTest(object):
    @classmethod
    def main(cls, args):
        # db = SimpleDB("txtest", 400, 8)
        fm = FileMgr(File("txtest"), 400)
        lm = LogMgr(fm, "simpledb.log")

        bm = BufferMgr(fm, lm, 8)

        tx1 = Transaction(fm, lm, bm)
        blk = BlockId("testfile", 1)
        tx1.pin(blk)
        #  The block initially contains unknown bytes,
        #  so don't log those values here.
        tx1.setInt(blk, 80, 1, False)
        tx1.setString(blk, 40, "one", False)
        tx1.commit()

        tx2 = Transaction(fm, lm, bm)
        tx2.pin(blk)
        ival = tx2.getInt(blk, 80)
        sval = tx2.getString(blk, 40)
        print("initial value at location 80 = " + str(ival))
        print("initial value at location 40 = " + str(sval))
        newival = ival + 1
        newsval = sval + "!"
        tx2.setInt(blk, 80, newival, True)
        tx2.setString(blk, 40, newsval, True)
        tx2.commit()

        tx3 = Transaction(fm, lm, bm)
        tx3.pin(blk)
        print("new value at location 80 = " + str(tx3.getInt(blk, 80)))
        print("new value at location 40 = " + tx3.getString(blk, 40))
        tx3.setInt(blk, 80, 9999, True)
        print("pre-rollback value at location 80 = " + str(tx3.getInt(blk, 80)))
        tx3.rollback()

        tx4 = Transaction(fm, lm, bm)
        tx4.pin(blk)
        print("post-rollback at location 80 = " + str(tx4.getInt(blk, 80)))
        tx4.commit()


if __name__ == '__main__':
    import sys
    TxTest.main(sys.argv)
