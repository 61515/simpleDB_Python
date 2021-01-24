#
#  * Provide transaction management for clients,
#  * ensuring that all transactions are serializable, recoverable,
#  * and in general satisfy the ACID properties.
#  * @author Edward Sciore
#
import threading

from simpledb.file.BlockId import BlockId
from simpledb.tx.BufferList import BufferList
from simpledb.tx.concurrency.ConcurrencyMgr import ConcurrencyMgr
from simpledb.tx.recovery.RecoveryMgr import RecoveryMgr
from simpledb.util.Synchronized import synchronized


class Transaction(object):
    nextTxNum = 0
    END_OF_FILE = -1
    lock = threading.Lock()

    #
    #     * Create a new transaction and its associated
    #     * recovery and concurrency managers.
    #     * This constructor depends on the file, log, and buffer
    #     * managers that it gets from the class
    #     * {@link simpledb.server.SimpleDB}.
    #     * Those objects are created during system initialization.
    #     * Thus this constructor cannot be called until either
    #     * {@link simpledb.server.SimpleDB#init(String)} or
    #     * {@link simpledb.server.SimpleDB#initFileLogAndBufferMgr(String)} or
    #     * is called first.
    #
    def __init__(self, fm, lm, bm):
        self.fm = fm
        self.bm = bm
        self.txnum = self.nextTxNumber()
        self.recoveryMgr = RecoveryMgr(self, self.txnum, lm, bm)
        self.concurMgr = ConcurrencyMgr()
        self.mybuffers = BufferList(bm)

    #
    #     * Commit the current transaction.
    #     * Flush all modified buffers (and their log records),
    #     * write and flush a commit record to the log,
    #     * release all locks, and unpin any pinned buffers.
    #
    def commit(self):
        self.recoveryMgr.commit()
        print("transaction " + str(self.txnum) + " committed")
        self.concurMgr.release()
        self.mybuffers.unpinAll()

    #
    #     * Rollback the current transaction.
    #     * Undo any modified values,
    #     * flush those buffers,
    #     * write and flush a rollback record to the log,
    #     * release all locks, and unpin any pinned buffers.
    #
    def rollback(self):
        self.recoveryMgr.rollback()
        print("transaction " + str(self.txnum) + " rolled back")
        self.concurMgr.release()
        self.mybuffers.unpinAll()

    #
    #     * Flush all modified buffers.
    #     * Then go through the log, rolling back all
    #     * uncommitted transactions.  Finally,
    #     * write a quiescent checkpoint record to the log.
    #     * This method is called during system startup,
    #     * before user transactions begin.
    #
    def recover(self):
        self.bm.flushAll(self.txnum)
        self.recoveryMgr.recover()

    #
    #     * Pin the specified block.
    #     * The transaction manages the buffer for the client.
    #     * @param blk a reference to the disk block
    #
    def pin(self, blk):
        self.mybuffers.pin(blk)

    #
    #     * Unpin the specified block.
    #     * The transaction looks up the buffer pinned to this block,
    #     * and unpins it.
    #     * @param blk a reference to the disk block
    #
    def unpin(self, blk):
        self.mybuffers.unpin(blk)

    #
    #     * Return the integer value stored at the
    #     * specified offset of the specified block.
    #     * The method first obtains an SLock on the block,
    #     * then it calls the buffer to retrieve the value.
    #     * @param blk a reference to a disk block
    #     * @param offset the byte offset within the block
    #     * @return the integer stored at that offset
    #
    def getInt(self, blk, offset):
        self.concurMgr.sLock(blk)
        buff = self.mybuffers.getBuffer(blk)
        return buff.contents().getInt(offset)

    #
    #     * Return the string value stored at the
    #     * specified offset of the specified block.
    #     * The method first obtains an SLock on the block,
    #     * then it calls the buffer to retrieve the value.
    #     * @param blk a reference to a disk block
    #     * @param offset the byte offset within the block
    #     * @return the string stored at that offset
    #
    def getString(self, blk, offset):
        self.concurMgr.sLock(blk)
        buff = self.mybuffers.getBuffer(blk)
        return buff.contents().getString(offset)

    #
    #     * Store an integer at the specified offset
    #     * of the specified block.
    #     * The method first obtains an XLock on the block.
    #     * It then reads the current value at that offset,
    #     * puts it into an update log record, and
    #     * writes that record to the log.
    #     * Finally, it calls the buffer to store the value,
    #     * passing in the LSN of the log record and the transaction's id.
    #     * @param blk a reference to the disk block
    #     * @param offset a byte offset within that block
    #     * @param val the value to be stored
    #
    def setInt(self, blk, offset, val, okToLog):
        self.concurMgr.xLock(blk)
        buff = self.mybuffers.getBuffer(blk)
        lsn = -1
        if okToLog:
            lsn = self.recoveryMgr.setInt(buff, offset, val)
        p = buff.contents()
        p.setInt(offset, val)
        buff.setModified(self.txnum, lsn)

    #
    #     * Store a string at the specified offset
    #     * of the specified block.
    #     * The method first obtains an XLock on the block.
    #     * It then reads the current value at that offset,
    #     * puts it into an update log record, and
    #     * writes that record to the log.
    #     * Finally, it calls the buffer to store the value,
    #     * passing in the LSN of the log record and the transaction's id.
    #     * @param blk a reference to the disk block
    #     * @param offset a byte offset within that block
    #     * @param val the value to be stored
    #
    def setString(self, blk, offset, val, okToLog):
        self.concurMgr.xLock(blk)
        buff = self.mybuffers.getBuffer(blk)
        lsn = -1
        if okToLog:
            lsn = self.recoveryMgr.setString(buff, offset, val)
        p = buff.contents()
        p.setString(offset, val)
        buff.setModified(self.txnum, lsn)

    #
    #     * Return the number of blocks in the specified file.
    #     * This method first obtains an SLock on the
    #     * "end of the file", before asking the file manager
    #     * to return the file size.
    #     * @param filename the name of the file
    #     * @return the number of blocks in the file
    #
    def size(self, filename):
        dummyblk = BlockId(filename, self.END_OF_FILE)
        self.concurMgr.sLock(dummyblk)
        return self.fm.length(filename)

    #
    #     * Append a new block to the end of the specified file
    #     * and returns a reference to it.
    #     * This method first obtains an XLock on the
    #     * "end of the file", before performing the append.
    #     * @param filename the name of the file
    #     * @return a reference to the newly-created disk block
    #
    def append(self, filename):
        dummyblk = BlockId(filename, self.END_OF_FILE)
        self.concurMgr.xLock(dummyblk)
        return self.fm.append(filename)

    def blockSize(self):
        return self.fm.blockSize()

    def availableBuffs(self):
        return self.bm.available()

    @staticmethod
    def nextTxNumber():
        with Transaction.lock:
            Transaction.nextTxNum += 1
            return Transaction.nextTxNum
