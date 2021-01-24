from simpledb.file.BlockId import BlockId
from simpledb.file.Page import Page
from simpledb.tx.recovery.LogRecord import LogRecord
from simpledb.util.Integer import Integer


class SetIntRecord(LogRecord):
    #
    #     * Create a new setint log record.
    #     * @param bb the bytebuffer containing the log values
    #
    def __init__(self, p):
        super(SetIntRecord, self).__init__()
        tpos = Integer.BYTES
        self.txnum = p.getInt(tpos)
        fpos = tpos + Integer.BYTES
        filename = p.getString(fpos)
        bpos = fpos + Page.maxLength(len(filename))
        blknum = p.getInt(bpos)
        self.blk = BlockId(filename, blknum)
        opos = bpos + Integer.BYTES
        self.offset = p.getInt(opos)
        vpos = opos + Integer.BYTES
        self.val = p.getInt(vpos)

    def op(self):
        return LogRecord.SETINT

    def txNumber(self):
        return self.txnum

    def __str__(self):
        return "<SETINT " + str(self.txnum) + " " + self.blk.__str__() + " " + str(self.offset) + " " \
               + str(self.val) + "> "

    #
    #     * Replace the specified data value with the value saved in the log record.
    #     * The method pins a buffer to the specified block,
    #     * calls setInt to restore the saved value,
    #     * and unpins the buffer.
    #     * @see LogRecord#undo(int)
    #
    def undo(self, tx):
        tx.pin(self.blk)
        tx.setInt(self.blk, self.offset, self.val, False)  # don't log the undo!
        tx.unpin(self.blk)

    #
    #     * A static method to write a setInt record to the log.
    #     * This log record contains the SETINT operator,
    #     * followed by the transaction id, the filename, number,
    #     * and offset of the modified block, and the previous
    #     * integer value at that offset.
    #     * @return the LSN of the last log value
    #
    @staticmethod
    def writeToLog(lm, txnum, blk, offset, val):
        tpos = Integer.BYTES
        fpos = tpos + Integer.BYTES
        bpos = fpos + Page.maxLength(len(blk.fileName()))
        opos = bpos + Integer.BYTES
        vpos = opos + Integer.BYTES
        rec = bytearray(vpos + Integer.BYTES)
        p = Page(rec)
        p.setInt(0, LogRecord.SETINT)
        p.setInt(tpos, txnum)
        p.setString(fpos, blk.fileName())
        p.setInt(bpos, blk.number())
        p.setInt(opos, offset)
        p.setInt(vpos, val)
        return lm.append(rec)
