#
#  * Manages the pinning and unpinning of buffers to blocks.
#  * @author Edward Sciore
#  *
#
from simpledb.buffer.Buffer import Buffer
from simpledb.buffer.BufferAbortException import BufferAbortException
from simpledb.util.Synchronized import synchronized
import threading
import time


class BufferMgr(object):
    MAX_TIME = 10  # 10 seconds

    #
    #     * Creates a buffer manager having the specified number
    #     * of buffer slots.
    #     * This constructor depends on a {@link FileMgr} and
    #     * {@link LogMgr LogMgr} object.
    #     * @param numbuffs the number of buffer slots to allocate
    #
    def __init__(self, fm, lm, numbuffs):
        self.bufferpool = [None] * numbuffs
        self.numAvailable = numbuffs
        for i in range(numbuffs):
            self.bufferpool[i] = Buffer(fm, lm)
        self.lock = threading.Lock()  # 同步锁
        self.condition = threading.Condition()  # 条件锁

    #
    #     * Returns the number of available (i.e. unpinned) buffers.
    #     * @return the number of available buffers
    #
    @synchronized
    def available(self):
        return self.numAvailable

    #
    #     * Flushes the dirty buffers modified by the specified transaction.
    #     * @param txnum the transaction's id number
    #
    @synchronized
    def flushAll(self, txnum):
        for buff in self.bufferpool:
            if buff.modifyingTx() == txnum:
                buff.flush()

    #
    #     * Unpins the specified data buffer. If its pin count
    #     * goes to zero, then notify any waiting threads.
    #     * @param buff the buffer to be unpinned
    #
    @synchronized
    def unpin(self, buff):
        buff.unpin()
        if not buff.isPinned():
            self.numAvailable += 1
            if self.condition.acquire():
                self.condition.notifyAll()
                self.condition.release()

    #
    #     * Pins a buffer to the specified block, potentially
    #     * waiting until a buffer becomes available.
    #     * If no buffer becomes available within a fixed
    #     * time period, then a {@link BufferAbortException} is thrown.
    #     * @param blk a reference to a disk block
    #     * @return the buffer pinned to that block
    #

    @synchronized
    def pin(self, blk):
        try:
            timestamp = int(round(time.time() * 1000))
            buff = self.tryToPin(blk)
            while buff is None and not self.waitingTooLong(timestamp):
                if self.condition.acquire():
                    self.condition.wait(BufferMgr.MAX_TIME)
                    buff = self.tryToPin(blk)
                    self.condition.release()
            if buff is None:
                raise BufferAbortException()
            return buff
        except InterruptedError as e:
            raise BufferAbortException()

    def waitingTooLong(self, starttime):
        return int(round(time.time() * 1000)) - starttime > BufferMgr.MAX_TIME

    #
    #     * Tries to pin a buffer to the specified block.
    #     * If there is already a buffer assigned to that block
    #     * then that buffer is used;
    #     * otherwise, an unpinned buffer from the pool is chosen.
    #     * Returns a null value if there are no available buffers.
    #     * @param blk a reference to a disk block
    #     * @return the pinned buffer
    #
    def tryToPin(self, blk):
        buff = self.findExistingBuffer(blk)
        if buff is None:
            buff = self.chooseUnpinnedBuffer()
            if buff is None:
                return None
            buff.assignToBlock(blk)
        if not buff.isPinned():
            self.numAvailable -= 1
        buff.pin()
        return buff

    def findExistingBuffer(self, blk):
        for buff in self.bufferpool:
            b = buff.block()
            if b is not None and b == blk:
                return buff
        return None

    def chooseUnpinnedBuffer(self):
        for buff in self.bufferpool:
            if not buff.isPinned():
                return buff
        return None
