from simpledb.file.Page import Page
from simpledb.tx.recovery.LogRecord import LogRecord
from simpledb.util.Integer import Integer


class StartRecord(LogRecord):

    #
    #     * Create a log record by reading one other value from the log.
    #     * @param bb the bytebuffer containing the log values
    #
    def __init__(self, p):
        super(StartRecord, self).__init__()
        tpos = Integer.BYTES
        self.txnum = p.getInt(tpos)

    def op(self):
        return LogRecord.START

    def txNumber(self):
        return self.txnum

    #
    #     * Does nothing, because a start record
    #     * contains no undo information.
    #
    def undo(self, tx):
        """ method undo """

    def __str__(self):
        return "<START " + str(self.txnum) + ">"

    #
    #     * A static method to write a start record to the log.
    #     * This log record contains the START operator,
    #     * followed by the transaction id.
    #     * @return the LSN of the last log value
    #
    @staticmethod
    def writeToLog(lm, txnum):
        rec = bytearray(2 * Integer.BYTES)
        p = Page(rec)
        p.setInt(0, LogRecord.START)
        p.setInt(Integer.BYTES, txnum)
        return lm.append(rec)
