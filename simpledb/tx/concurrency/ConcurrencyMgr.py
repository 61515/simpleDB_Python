#
#  * The concurrency manager for the transaction.
#  * Each transaction has its own concurrency manager.
#  * The concurrency manager keeps track of which locks the
#  * transaction currently has, and interacts with the
#  * global lock table as needed.
#  * @author Edward Sciore
#
from simpledb.tx.concurrency.LockTable import LockTable


class ConcurrencyMgr(object):
    #
    #     * The global lock table. This variable is static because
    #     * all transactions share the same table.
    #

    locktbl = LockTable()

    def __init__(self):
        self.locks = {}

    #
    #     * Obtain an SLock on the block, if necessary.
    #     * The method will ask the lock table for an SLock
    #     * if the transaction currently has no locks on that block.
    #     * @param blk a reference to the disk block
    #
    def sLock(self, blk):
        if self.locks.get(blk) is None:
            ConcurrencyMgr.locktbl.sLock(blk)
            self.locks[blk] = "S"

    #
    #     * Obtain an XLock on the block, if necessary.
    #     * If the transaction does not have an XLock on that block,
    #     * then the method first gets an SLock on that block
    #     * (if necessary), and then upgrades it to an XLock.
    #     * @param blk a reference to the disk block
    #
    def xLock(self, blk):
        if not self.hasXLock(blk):
            self.sLock(blk)
            ConcurrencyMgr.locktbl.xLock(blk)
            self.locks[blk] = "X"

    #
    #     * Release all locks by asking the lock table to
    #     * unlock each one.
    #

    def release(self):
        for blk in self.locks.keys():
            ConcurrencyMgr.locktbl.unlock(blk)
        self.locks.clear()

    def hasXLock(self, blk):
        locktype = self.locks.get(blk)
        return locktype is not None and locktype == "X"
