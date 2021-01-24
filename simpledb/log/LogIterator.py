#
#  * A class that provides the ability to move through the
#  * records of the log file in reverse order.
#  *
#  * @author Edward Sciore
#
from simpledb.file.BlockId import BlockId
from simpledb.file.Page import Page
from simpledb.util.Integer import Integer


class LogIterator:
    #
    #     * Creates an iterator for the records in the log file,
    #     * positioned after the last log record.
    #
    def __init__(self, fm, blk):
        self.fm = fm
        self.blk = blk
        self.currentpos = 0
        self.boundary = 0
        b = bytearray(fm.blockSize())
        self.p = Page(b)
        self.moveToBlock(blk)

    #
    #     * Determines if the current log record
    #     * is the earliest record in the log file.
    #     * @return true if there is an earlier record
    #
    def hasNext(self):
        return self.currentpos < self.fm.blockSize() or self.blk.number() > 0

    #
    #     * Moves to the next log record in the block.
    #     * If there are no more log records in the block,
    #     * then move to the previous block
    #     * and return the log record from there.
    #     * @return the next earliest log record
    #
    def next(self):
        if self.currentpos == self.fm.blockSize():
            self.blk = BlockId(self.blk.fileName(), self.blk.number() - 1)
            self.moveToBlock(self.blk)
        rec = self.p.getBytes(self.currentpos)
        self.currentpos += Integer.BYTES + len(rec)
        return rec

    #
    #     * Moves to the specified log block
    #     * and positions it at the first record in that block
    #     * (i.e., the most recent one).
    #
    def moveToBlock(self, blk):
        self.fm.read(blk, self.p)
        self.boundary = self.p.getInt(0)
        self.currentpos = self.boundary
