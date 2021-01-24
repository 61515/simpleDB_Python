#
#  * B-tree directory and leaf pages have many commonalities:
#  * in particular, their records are stored in sorted order,
#  * and pages split when full.
#  * A BTNode object contains this common functionality.
#  * @author Edward Sciore
#
from simpledb.query.Constant import Constant
from simpledb.record.RID import RID
from simpledb.util.Integer import Integer
from simpledb.util.Types import INTEGER


class BTPage(object):

    #
    #     * Open a node for the specified B-tree block.
    #     * @param currentblk a reference to the B-tree block
    #     * @param layout the metadata for the particular B-tree file
    #     * @param tx the calling transaction
    #
    def __init__(self, tx, currentblk, layout):
        self.tx = tx
        self.currentblk = currentblk
        self.layout = layout
        tx.pin(currentblk)

    #
    #     * Calculate the position where the first record having
    #     * the specified search key should be, then returns
    #     * the position before it.
    #     * @param searchkey the search key
    #     * @return the position before where the search key goes
    #
    def findSlotBefore(self, searchkey):
        slot = 0
        while slot < self.getNumRecs() and self.getDataVal(slot).__cmp__(searchkey) < 0:
            slot += 1
        return slot - 1

    #
    #     * Close the page by unpinning its buffer.
    #
    def close(self):
        if self.currentblk is not None:
            self.tx.unpin(self.currentblk)
        self.currentblk = None

    #
    #     * Return true if the block is full.
    #     * @return true if the block is full
    #
    def isFull(self):
        return self.slotpos(self.getNumRecs() + 1) >= self.tx.blockSize()

    #
    #     * Split the page at the specified position.
    #     * A new page is created, and the records of the page
    #     * starting at the split position are transferred to the new page.
    #     * @param splitpos the split position
    #     * @param flag the initial value of the flag field
    #     * @return the reference to the new block
    #
    def split(self, splitpos, flag):
        newblk = self.appendNew(flag)
        newpage = BTPage(self.tx, newblk, self.layout)
        self.transferRecs(splitpos, newpage)
        newpage.setFlag(flag)
        newpage.close()
        return newblk

    #
    #     * Return the dataval of the record at the specified slot.
    #     * @param slot the integer slot of an index record
    #     * @return the dataval of the record at that slot
    #
    def getDataVal(self, slot):
        return self.getVal(slot, "dataval")

    #
    #     * Return the value of the page's flag field
    #     * @return the value of the page's flag field
    #
    def getFlag(self):
        return self.tx.getInt(self.currentblk, 0)

    #
    #     * Set the page's flag field to the specified value
    #     * @param val the new value of the page flag
    #
    def setFlag(self, val):
        self.tx.setInt(self.currentblk, 0, val, True)

    #
    #     * Append a new block to the end of the specified B-tree file,
    #     * having the specified flag value.
    #     * @param flag the initial value of the flag
    #     * @return a reference to the newly-created block
    #
    def appendNew(self, flag):
        blk = self.tx.append(self.currentblk.fileName())
        self.tx.pin(blk)
        self.format(blk, flag)
        return blk

    def format(self, blk, flag):
        self.tx.setInt(blk, 0, flag, False)
        self.tx.setInt(blk, Integer.BYTES, 0, False)  # #records = 0
        recsize = self.layout.slotSize()
        for pos in range(2 * Integer.BYTES, self.tx.blockSize() + 1, recsize):
            if pos + recsize > self.tx.blockSize():
                break
            self.makeDefaultRecord(blk, pos)

    def makeDefaultRecord(self, blk, pos):
        for fldname in self.layout.schema().fields():
            offset = self.layout.offset(fldname)
            if self.layout.schema().type(fldname) == INTEGER:
                self.tx.setInt(blk, pos + offset, 0, False)
            else:
                self.tx.setString(blk, pos + offset, "", False)

    #  Methods called only by BTreeDir
    #
    #     * Return the block number stored in the index record
    #     * at the specified slot.
    #     * @param slot the slot of an index record
    #     * @return the block number stored in that record
    #
    def getChildNum(self, slot):
        return self.getInt(slot, "block")

    #
    #     * Insert a directory entry at the specified slot.
    #     * @param slot the slot of an index record
    #     * @param val the dataval to be stored
    #     * @param blknum the block number to be stored
    #
    def insertDir(self, slot, val, blknum):
        self.insert(slot)
        self.setVal(slot, "dataval", val)
        self.setInt(slot, "block", blknum)

    #  Methods called only by BTreeLeaf
    #
    #     * Return the dataRID value stored in the specified leaf index record.
    #     * @param slot the slot of the desired index record
    #     * @return the dataRID value store at that slot
    #
    def getDataRid(self, slot):
        return RID(self.getInt(slot, "block"), self.getInt(slot, "id"))

    #
    #     * Insert a leaf index record at the specified slot.
    #     * @param slot the slot of the desired index record
    #     * @param val the new dataval
    #     * @param rid the new dataRID
    #
    def insertLeaf(self, slot, val, rid):
        self.insert(slot)
        self.setVal(slot, "dataval", val)
        self.setInt(slot, "block", rid.blockNumber())
        self.setInt(slot, "id", rid.slot())

    #
    #     * Delete the index record at the specified slot.
    #     * @param slot the slot of the deleted index record
    #
    def delete(self, slot):
        for i in range(slot + 1, self.getNumRecs()):
            self.copyRecord(i, i - 1)
        self.setNumRecs(self.getNumRecs() - 1)
        return

    #
    #     * Return the number of index records in this page.
    #     * @return the number of index records in this page
    #
    def getNumRecs(self):
        return self.tx.getInt(self.currentblk, Integer.BYTES)

    #  Private methods

    def getInt(self, slot, fldname):
        pos = self.fldpos(slot, fldname)
        return self.tx.getInt(self.currentblk, pos)

    def getString(self, slot, fldname):
        pos = self.fldpos(slot, fldname)
        return self.tx.getString(self.currentblk, pos)

    def getVal(self, slot, fldname):
        _type = self.layout.schema().type(fldname)
        if _type == INTEGER:
            return Constant(self.getInt(slot, fldname))
        else:
            return Constant(self.getString(slot, fldname))

    def setInt(self, slot, fldname, val):
        pos = self.fldpos(slot, fldname)
        self.tx.setInt(self.currentblk, pos, val, True)

    def setString(self, slot, fldname, val):
        pos = self.fldpos(slot, fldname)
        self.tx.setString(self.currentblk, pos, val, True)

    def setVal(self, slot, fldname, val):
        _type = self.layout.schema().type(fldname)
        if _type == INTEGER:
            self.setInt(slot, fldname, val.asInt())
        else:
            self.setString(slot, fldname, val.asString())

    def setNumRecs(self, n):
        self.tx.setInt(self.currentblk, Integer.BYTES, n, True)

    def insert(self, slot):
        for i in range(self.getNumRecs(), slot, -1):
            self.copyRecord(i - 1, i)
        self.setNumRecs(self.getNumRecs() + 1)

    def copyRecord(self, _from, to):
        sch = self.layout.schema()
        for fldname in sch.fields():
            self.setVal(to, fldname, self.getVal(_from, fldname))

    def transferRecs(self, slot, dest):
        destslot = 0
        while slot < self.getNumRecs():
            dest.insert(destslot)
            sch = self.layout.schema()
            for fldname in sch.fields():
                dest.setVal(destslot, fldname, self.getVal(slot, fldname))
            self.delete(slot)
            destslot += 1

    def fldpos(self, slot, fldname):
        offset = self.layout.offset(fldname)
        return self.slotpos(slot) + offset

    def slotpos(self, slot):
        slotsize = self.layout.slotSize()
        return Integer.BYTES + Integer.BYTES + (slot * slotsize)
