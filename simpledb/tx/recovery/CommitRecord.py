#
#  * The COMMIT log record
#  * @author Edward Sciore
#
from simpledb.file.Page import Page
from simpledb.tx.recovery.LogRecord import LogRecord
from simpledb.util.Integer import Integer


class CommitRecord(LogRecord):

    def __init__(self, p):
        super(CommitRecord, self).__init__()
        tpos = Integer.BYTES
        self.txnum = p.getInt(tpos)

    def op(self):
        return LogRecord.COMMIT

    def txNumber(self):
        return self.txnum

    #
    #     * Does nothing, because a commit record
    #     * contains no undo information.
    #
    def undo(self, tx):
        """ method undo """

    def __str__(self):
        return "<COMMIT " + str(self.txnum) + ">"

    #
    #     * A static method to write a commit record to the log.
    #     * This log record contains the COMMIT operator,
    #     * followed by the transaction id.
    #     * @return the LSN of the last log value
    #
    @staticmethod
    def writeToLog(lm, txnum):
        rec = bytearray(2 * Integer.BYTES)
        p = Page(rec)
        p.setInt(0, LogRecord.COMMIT)
        p.setInt(Integer.BYTES, txnum)
        return lm.append(rec)
