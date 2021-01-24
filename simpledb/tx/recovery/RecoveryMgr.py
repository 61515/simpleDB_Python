
#
#  * The recovery manager.  Each transaction has its own recovery manager.
#  * @author Edward Sciore
#
from simpledb.tx.recovery.CheckpointRecord import CheckpointRecord
from simpledb.tx.recovery.CommitRecord import CommitRecord
from simpledb.tx.recovery.LogRecord import LogRecord
from simpledb.tx.recovery.RollbackRecord import RollbackRecord
from simpledb.tx.recovery.SetIntRecord import SetIntRecord
from simpledb.tx.recovery.SetStringRecord import SetStringRecord
from simpledb.tx.recovery.StartRecord import StartRecord


class RecoveryMgr(object):

    #
    #     * Create a recovery manager for the specified transaction.
    #     * @param txnum the ID of the specified transaction
    #
    def __init__(self, tx, txnum, lm, bm):
        self.tx = tx
        self.txnum = txnum
        self.lm = lm
        self.bm = bm
        StartRecord.writeToLog(lm, txnum)

    #
    #     * Write a commit record to the log, and flushes it to disk.
    #
    def commit(self):
        self.bm.flushAll(self.txnum)
        lsn = CommitRecord.writeToLog(self.lm, self.txnum)
        self.lm.flush(lsn)

    #
    #     * Write a rollback record to the log and flush it to disk.
    #
    def rollback(self):
        self.doRollback()
        self.bm.flushAll(self.txnum)
        lsn = RollbackRecord.writeToLog(self.lm, self.txnum)
        self.lm.flush(lsn)

    #
    #     * Recover uncompleted transactions from the log
    #     * and then write a quiescent checkpoint record to the log and flush it.
    #
    def recover(self):
        self.doRecover()
        self.bm.flushAll(self.txnum)
        lsn = CheckpointRecord.writeToLog(self.lm)
        self.lm.flush(lsn)

    #
    #     * Write a setint record to the log and return its lsn.
    #     * @param buff the buffer containing the page
    #     * @param offset the offset of the value in the page
    #     * @param newval the value to be written
    #
    def setInt(self, buff, offset, newval):
        oldval = buff.contents().getInt(offset)
        blk = buff.block()
        return SetIntRecord.writeToLog(self.lm, self.txnum, blk, offset, oldval)

    #
    #     * Write a setstring record to the log and return its lsn.
    #     * @param buff the buffer containing the page
    #     * @param offset the offset of the value in the page
    #     * @param newval the value to be written
    #
    def setString(self, buff, offset, newval):
        oldval = buff.contents().getString(offset)
        blk = buff.block()
        return SetStringRecord.writeToLog(self.lm, self.txnum, blk, offset, oldval)

    #
    #     * Rollback the transaction, by iterating
    #     * through the log records until it finds
    #     * the transaction's START record,
    #     * calling undo() for each of the transaction's
    #     * log records.
    #
    def doRollback(self):
        iterator = self.lm.iterator()
        while iterator.hasNext():
            byte_array = iterator.next()
            rec = LogRecord.createLogRecord(byte_array)
            if rec.txNumber() == self.txnum:
                if rec.op() == LogRecord.START:
                    return
                rec.undo(self.tx)

    #
    #     * Do a complete database recovery.
    #     * The method iterates through the log records.
    #     * Whenever it finds a log record for an unfinished
    #     * transaction, it calls undo() on that record.
    #     * The method stops when it encounters a CHECKPOINT record
    #     * or the end of the log.
    #
    def doRecover(self):
        finishedTxs = set()
        iterator = self.lm.iterator()
        while iterator.hasNext():
            byte_array = iterator.next()
            rec = LogRecord.createLogRecord(byte_array)
            if rec.op() == LogRecord.CHECKPOINT:
                return
            if rec.op() == LogRecord.COMMIT or rec.op() == LogRecord.ROLLBACK:
                finishedTxs.add(rec.txNumber())
            elif rec.txNumber() not in finishedTxs:
                rec.undo(self.tx)
