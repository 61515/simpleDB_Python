#
#  * The Scan class for the multi-buffer version of the
#  * <i>product</i> operator.
#  * @author Edward Sciore
#
from simpledb.multibuffer.BufferNeeds import BufferNeeds
from simpledb.multibuffer.ChunkScan import ChunkScan
from simpledb.query.ProductScan import ProductScan
from simpledb.query.Scan import Scan


class MultibufferProductScan(Scan):

    #
    #     * Creates the scan class for the product of the LHS scan and a table.
    #     * @param lhsscan the LHS scan
    #     * @param layout the metadata for the RHS table
    #     * @param tx the current transaction
    #
    def __init__(self, tx, lhsscan, filename, layout):
        super(MultibufferProductScan, self).__init__()
        self.tx = tx
        self.lhsscan = lhsscan
        self.filename = filename
        self.layout = layout
        self.filesize = tx.size(filename)
        available = tx.availableBuffs()
        self.chunksize = BufferNeeds.bestFactor(available, self.filesize)
        self.beforeFirst()

    #
    #     * Positions the scan before the first record.
    #     * That is, the LHS scan is positioned at its first record,
    #     * and the RHS scan is positioned before the first record of the first chunk.
    #     * @see Scan#beforeFirst()
    #
    def beforeFirst(self):
        self.nextblknum = 0
        self.useNextChunk()

    #
    #     * Moves to the next record in the current scan.
    #     * If there are no more records in the current chunk,
    #     * then move to the next LHS record and the beginning of that chunk.
    #     * If there are no more LHS records, then move to the next chunk
    #     * and begin again.
    #     * @see Scan#next()
    #
    def next(self):
        while not self.prodscan.next():
            if not self.useNextChunk():
                return False
        return True

    #
    #     * Closes the current scans.
    #     * @see Scan#close()
    #
    def close(self):
        self.prodscan.close()

    #
    #     * Returns the value of the specified field.
    #     * The value is obtained from whichever scan
    #     * contains the field.
    #     * @see Scan#getVal(String)
    #
    def getVal(self, fldname):
        return self.prodscan.getVal(fldname)

    #
    #     * Returns the integer value of the specified field.
    #     * The value is obtained from whichever scan
    #     * contains the field.
    #     * @see Scan#getInt(String)
    #
    def getInt(self, fldname):
        return self.prodscan.getInt(fldname)

    #
    #     * Returns the string value of the specified field.
    #     * The value is obtained from whichever scan
    #     * contains the field.
    #     * @see Scan#getString(String)
    #
    def getString(self, fldname):
        return self.prodscan.getString(fldname)

    #
    #     * Returns true if the specified field is in
    #     * either of the underlying scans.
    #     * @see Scan#hasField(String)
    #
    def hasField(self, fldname):
        return self.prodscan.hasField(fldname)

    def useNextChunk(self):
        if self.rhsscan is not None:
            self.rhsscan.close()
        if self.nextblknum >= self.filesize:
            return False
        end = self.nextblknum + self.chunksize - 1
        if end >= self.filesize:
            end = self.filesize - 1
        self.rhsscan = ChunkScan(self.tx, self.filename, self.layout, self.nextblknum, end)
        self.lhsscan.beforeFirst()
        self.prodscan = ProductScan(self.lhsscan, self.rhsscan)
        self.nextblknum = end + 1
        return True
