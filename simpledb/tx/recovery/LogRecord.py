#
#  * The interface implemented by each type of log record.
#  * @author Edward Sciore
#
from simpledb.file.Page import Page
import abc


class LogRecord(metaclass=abc.ABCMeta):
    CHECKPOINT = 0
    START = 1
    COMMIT = 2
    ROLLBACK = 3
    SETINT = 4
    SETSTRING = 5

    #
    #     * Returns the log record's type.
    #     * @return the log record's type
    #
    @abc.abstractmethod
    def op(self):
        """return the log record's type"""

    #
    #     * Returns the transaction id stored with
    #     * the log record.
    #     * @return the log record's transaction id
    #
    @abc.abstractmethod
    def txNumber(self):
        """return the log record's transaction id"""

    #
    #     * Undoes the operation encoded by this log record.
    #     * The only log record types for which this method
    #     * does anything interesting are SETINT and SETSTRING.
    #     * @param txnum the id of the transaction that is performing the undo.
    #
    @abc.abstractmethod
    def undo(self, tx):
        """Undoes the operation encoded by this log record"""

    #
    #     * Interpret the bytes returned by the log iterator.
    #     * @param bytes
    #     * @return
    #
    @staticmethod
    def createLogRecord(byte_array):
        p = Page(byte_array)
        if p.getInt(0) == LogRecord.CHECKPOINT:
            from simpledb.tx.recovery.CheckpointRecord import CheckpointRecord
            return CheckpointRecord()
        elif p.getInt(0) == LogRecord.START:
            from simpledb.tx.recovery.StartRecord import StartRecord
            return StartRecord(p)
        elif p.getInt(0) == LogRecord.COMMIT:
            from simpledb.tx.recovery.CommitRecord import CommitRecord
            return CommitRecord(p)
        elif p.getInt(0) == LogRecord.ROLLBACK:
            from simpledb.tx.recovery.RollbackRecord import RollbackRecord
            return RollbackRecord(p)
        elif p.getInt(0) == LogRecord.SETINT:
            from simpledb.tx.recovery.SetIntRecord import SetIntRecord
            return SetIntRecord(p)
        elif p.getInt(0) == LogRecord.SETSTRING:
            from simpledb.tx.recovery.SetStringRecord import SetStringRecord
            return SetStringRecord(p)
        else:
            return None
