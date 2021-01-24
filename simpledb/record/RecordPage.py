
#
#  * Store a record at a given location in a block.
#  * @author Edward Sciore
#
from simpledb.util.Types import INTEGER


class RecordPage(object):
    EMPTY = 0
    USED = 1

    def __init__(self, tx, blk, layout):
        self.tx = tx
        self.blk = blk
        self.layout = layout
        tx.pin(blk)

    #
    #     * Return the integer value stored for the
    #     * specified field of a specified slot.
    #     * @param fldname the name of the field.
    #     * @return the integer stored in that field
    #
    def getInt(self, slot, fldname):
        fldpos = self.offset(slot) + self.layout.offset(fldname)
        return self.tx.getInt(self.blk, fldpos)

    #
    #     * Return the string value stored for the
    #     * specified field of the specified slot.
    #     * @param fldname the name of the field.
    #     * @return the string stored in that field
    #
    def getString(self, slot, fldname):
        fldpos = self.offset(slot) + self.layout.offset(fldname)
        return self.tx.getString(self.blk, fldpos)

    #
    #     * Store an integer at the specified field
    #     * of the specified slot.
    #     * @param fldname the name of the field
    #     * @param val the integer value stored in that field
    #
    def setInt(self, slot, fldname, val):
        fldpos = self.offset(slot) + self.layout.offset(fldname)
        self.tx.setInt(self.blk, fldpos, val, True)

    #
    #     * Store a string at the specified field
    #     * of the specified slot.
    #     * @param fldname the name of the field
    #     * @param val the string value stored in that field
    #
    def setString(self, slot, fldname, val):
        fldpos = self.offset(slot) + self.layout.offset(fldname)
        self.tx.setString(self.blk, fldpos, val, True)

    def delete(self, slot):
        self.setFlag(slot, self.EMPTY)

    #     *  Use the layout to format a new block of records.
    #     *  These values should not be logged
    #     *  (because the old values are meaningless).
    #
    def format(self):
        slot = 0
        while self.isValidSlot(slot):
            self.tx.setInt(self.blk, self.offset(slot), self.EMPTY, False)
            sch = self.layout.schema()
            for fldname in sch.fields():
                fldpos = self.offset(slot) + self.layout.offset(fldname)
                if sch.type(fldname) == INTEGER:
                    self.tx.setInt(self.blk, fldpos, 0, False)
                else:
                    self.tx.setString(self.blk, fldpos, "", False)
            slot += 1

    def nextAfter(self, slot):
        return self.searchAfter(slot, self.USED)

    def insertAfter(self, slot):
        newslot = self.searchAfter(slot, self.EMPTY)
        if newslot >= 0:
            self.setFlag(newslot, self.USED)
        return newslot

    def block(self):
        return self.blk

    #  Private auxiliary methods
    #
    #     * Set the record's empty/inuse flag.
    #
    def setFlag(self, slot, flag):
        self.tx.setInt(self.blk, self.offset(slot), flag, True)

    def searchAfter(self, slot, flag):
        slot += 1
        while self.isValidSlot(slot):
            if self.tx.getInt(self.blk, self.offset(slot)) == flag:
                return slot
            slot += 1
        return -1

    def isValidSlot(self, slot):
        return self.offset(slot + 1) <= self.tx.blockSize()

    def offset(self, slot):
        return slot * self.layout.slotSize()
