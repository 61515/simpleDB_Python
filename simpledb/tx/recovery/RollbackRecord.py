#
#  * The ROLLBACK log record.
#  * @author Edward Sciore
#
from simpledb.file.Page import Page
from simpledb.tx.recovery.LogRecord import LogRecord
from simpledb.util.Integer import Integer


class RollbackRecord(LogRecord):

    #
    #     * Create a RollbackRecord object.
    #     * @param txnum the ID of the specified transaction
    #
    def __init__(self, p):
        super(RollbackRecord, self).__init__()
        tpos = Integer.BYTES
        self.txnum = p.getInt(tpos)

    def op(self):
        return LogRecord.ROLLBACK

    def txNumber(self):
        return self.txnum

    #
    #     * Does nothing, because a rollback record
    #     * contains no undo information.
    #
    def undo(self, tx):
        """ method undo """

    def __str__(self):
        return "<ROLLBACK " + str(self.txnum) + ">"

    #
    #     * A static method to write a rollback record to the log.
    #     * This log record contains the ROLLBACK operator,
    #     * followed by the transaction id.
    #     * @return the LSN of the last log value
    #
    @staticmethod
    def writeToLog(lm, txnum):
        rec = bytearray(2 * Integer.BYTES)
        p = Page(rec)
        p.setInt(0, LogRecord.ROLLBACK)
        p.setInt(Integer.BYTES, txnum)
        return lm.append(rec)
