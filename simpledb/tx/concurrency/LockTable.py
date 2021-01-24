#
#  * The lock table, which provides methods to lock and unlock blocks.
#  * If a transaction requests a lock that causes a conflict with an
#  * existing lock, then that transaction is placed on a wait list.
#  * There is only one wait list for all blocks.
#  * When the last lock on a block is unlocked, then all transactions
#  * are removed from the wait list and rescheduled.
#  * If one of those transactions discovers that the lock it is waiting for
#  * is still locked, it will place itself back on the wait list.
#  * @author Edward Sciore
#
from simpledb.tx.concurrency.LockAbortException import LockAbortException
from simpledb.util.Synchronized import synchronized
import time
import threading


class LockTable(object):
    MAX_TIME = 10000  # 10 seconds
    MAX_TIME_s = 10

    def __init__(self):
        self.locks = {}
        self.lock = threading.Lock()
        self.condition = threading.Condition()  # 条件锁

    #
    #     * Grant an SLock on the specified block.
    #     * If an XLock exists when the method is called,
    #     * then the calling thread will be placed on a wait list
    #     * until the lock is released.
    #     * If the thread remains on the wait list for a certain
    #     * amount of time (currently 10 seconds),
    #     * then an exception is thrown.
    #     * @param blk a reference to the disk block
    #
    @synchronized
    def sLock(self, blk):
        try:
            timestamp = int(round(time.time() * 1000))
            while self.hasXlock(blk) and not self.waitingTooLong(timestamp):
                if self.condition.acquire():
                    self.lock.release()
                    self.condition.wait(LockTable.MAX_TIME_s)
                    self.lock.acquire()
                    self.condition.release()

            if self.hasXlock(blk):
                raise LockAbortException()
            val = self.getLockVal(blk)  # will not be negative
            self.locks[blk] = val + 1
        except InterruptedError as e:
            raise LockAbortException()

    #
    #     * Grant an XLock on the specified block.
    #     * If a lock of any type exists when the method is called,
    #     * then the calling thread will be placed on a wait list
    #     * until the locks are released.
    #     * If the thread remains on the wait list for a certain
    #     * amount of time (currently 10 seconds),
    #     * then an exception is thrown.
    #     * @param blk a reference to the disk block
    #
    @synchronized
    def xLock(self, blk):
        try:
            timestamp = int(round(time.time() * 1000))
            while self.hasOtherSLocks(blk) and not self.waitingTooLong(timestamp):
                if self.condition.acquire():
                    self.lock.release()
                    self.condition.wait(LockTable.MAX_TIME_s)
                    self.lock.acquire()
                    self.condition.release()

            if self.hasOtherSLocks(blk):
                raise LockAbortException()
            self.locks[blk] = -1
        except InterruptedError as e:
            raise LockAbortException()

    #
    #     * Release a lock on the specified block.
    #     * If this lock is the last lock on that block,
    #     * then the waiting transactions are notified.
    #     * @param blk a reference to the disk block
    #
    @synchronized
    def unlock(self, blk):
        val = self.getLockVal(blk)
        if val > 1:
            self.locks[blk] = val - 1
        else:
            self.locks.pop(blk)
            if self.condition.acquire():
                self.condition.notifyAll()
                self.condition.release()

    def hasXlock(self, blk):
        return self.getLockVal(blk) < 0

    def hasOtherSLocks(self, blk):
        return self.getLockVal(blk) > 1

    def waitingTooLong(self, starttime):
        return int(round(time.time() * 1000)) - starttime > LockTable.MAX_TIME

    def getLockVal(self, blk):
        ival = self.locks.get(blk)
        return 0 if (ival is None) else int(ival)
