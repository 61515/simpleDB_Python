#
#  * The class for the <i>chunk</i> operator.
#  * @author Edward Sciore
#
from simpledb.file.BlockId import BlockId
from simpledb.query.Constant import Constant
from simpledb.query.Scan import Scan
from simpledb.record.RecordPage import RecordPage
from simpledb.util.Types import INTEGER


class ChunkScan(Scan):

    #
    #     * Create a chunk consisting of the specified pages.
    #     * @param layout the metadata for the chunked table
    #     * @param startbnum the starting block number
    #     * @param endbnum  the ending block number
    #     * @param tx the current transaction
    #
    def __init__(self, tx, filename, layout, startbnum, endbnum):
        super(ChunkScan, self).__init__()
        self.tx = tx
        self.filename = filename
        self.layout = layout
        self.startbnum = startbnum
        self.endbnum = endbnum
        self.buffs = []
        for i in range(startbnum, endbnum + 1):
            blk = BlockId(filename, i)
            self.buffs.append(RecordPage(tx, blk, layout))
        self.moveToBlock(startbnum)

    #
    #     * @see Scan#close()
    #
    def close(self):
        for i in range(len(self.buffs)):
            blk = BlockId(self.filename, self.startbnum + i)
            self.tx.unpin(blk)

    #
    #     * @see Scan#beforeFirst()
    #
    def beforeFirst(self):
        self.moveToBlock(self.startbnum)

    #
    #     * Moves to the next record in the current block of the chunk.
    #     * If there are no more records, then make
    #     * the next block be current.
    #     * If there are no more blocks in the chunk, return false.
    #     * @see Scan#next()
    #
    def next(self):
        self.currentslot = self.rp.nextAfter(self.currentslot)
        while self.currentslot < 0:
            if self.currentbnum == self.endbnum:
                return False
            self.moveToBlock(self.rp.block().number() + 1)
            self.currentslot = self.rp.nextAfter(self.currentslot)
        return True

    #
    #     * @see Scan#getInt(String)
    #
    def getInt(self, fldname):
        return self.rp.getInt(self.currentslot, fldname)

    #
    #     * @see Scan#getString(String)
    #
    def getString(self, fldname):
        return self.rp.getString(self.currentslot, fldname)

    #
    #     * @see Scan#getVal(String)
    #
    def getVal(self, fldname):
        if self.layout.schema().type(fldname) == INTEGER:
            return Constant(self.getInt(fldname))
        else:
            return Constant(self.getString(fldname))

    #
    #     * @see Scan#hasField(String)
    #
    def hasField(self, fldname):
        return self.layout.schema().hasField(fldname)

    def moveToBlock(self, blknum):
        self.currentbnum = blknum
        self.rp = self.buffs[self.currentbnum - self.startbnum]
        self.currentslot = -1
