#
#  * The CHECKPOINT log record.
#  * @author Edward Sciore
#
from simpledb.file.Page import Page
from simpledb.tx.recovery.LogRecord import LogRecord
from simpledb.util.Integer import Integer


class CheckpointRecord(LogRecord):
    def __init__(self):
        super(CheckpointRecord, self).__init__()

    def op(self):
        return LogRecord.CHECKPOINT

    #
    #     * Checkpoint records have no associated transaction,
    #     * and so the method returns a "dummy", negative txid.
    #
    def txNumber(self):
        return -1  # dummy value

    #
    #     * Does nothing, because a checkpoint record
    #     * contains no undo information.
    #
    def undo(self, tx):
        """ method undo """

    def __str__(self):
        return "<CHECKPOINT>"

    #
    #     * A static method to write a checkpoint record to the log.
    #     * This log record contains the CHECKPOINT operator,
    #     * and nothing else.
    #     * @return the LSN of the last log value
    #
    @staticmethod
    def writeToLog(lm):
        rec = bytearray(Integer.BYTES)
        p = Page(rec)
        p.setInt(0, LogRecord.CHECKPOINT)
        return lm.append(rec)
