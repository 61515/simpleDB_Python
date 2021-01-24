#
#  * Provides the abstraction of an arbitrarily large array
#  * of records.
#  * @author sciore
#
from simpledb.file.BlockId import BlockId
from simpledb.query.UpdateScan import UpdateScan
from simpledb.record.RID import RID
from simpledb.record.RecordPage import RecordPage
from simpledb.query.Constant import Constant
from simpledb.util.Types import INTEGER


class TableScan(UpdateScan):

    def __init__(self, tx, tblname, layout):
        super(TableScan, self).__init__()
        self.tx = tx
        self.layout = layout
        self.filename = tblname + ".tbl"
        self.rp = None
        if tx.size(self.filename) == 0:
            self.moveToNewBlock()
        else:
            self.moveToBlock(0)

    #  Methods that implement Scan
    def beforeFirst(self):
        self.moveToBlock(0)

    def next(self):
        self.currentslot = self.rp.nextAfter(self.currentslot)
        while self.currentslot < 0:
            if self.atLastBlock():
                return False
            self.moveToBlock(self.rp.block().number() + 1)
            self.currentslot = self.rp.nextAfter(self.currentslot)
        return True

    def getInt(self, fldname):
        return self.rp.getInt(self.currentslot, fldname)

    def getString(self, fldname):
        return self.rp.getString(self.currentslot, fldname)

    def getVal(self, fldname):
        if self.layout.schema().type(fldname) == INTEGER:
            return Constant(self.getInt(fldname))
        else:
            return Constant(self.getString(fldname))

    def hasField(self, fldname):
        return self.layout.schema().hasField(fldname)

    def close(self):
        if self.rp is not None:
            self.tx.unpin(self.rp.block())

    #  Methods that implement UpdateScan
    def setInt(self, fldname, val):
        self.rp.setInt(self.currentslot, fldname, val)

    def setString(self, fldname, val):
        self.rp.setString(self.currentslot, fldname, val)

    def setVal(self, fldname, val):
        if self.layout.schema().type(fldname) == INTEGER:
            self.setInt(fldname, val.asInt())
        else:
            self.setString(fldname, val.asString())

    def insert(self):
        self.currentslot = self.rp.insertAfter(self.currentslot)
        while self.currentslot < 0:
            if self.atLastBlock():
                self.moveToNewBlock()
            else:
                self.moveToBlock(self.rp.block().number() + 1)
            self.currentslot = self.rp.insertAfter(self.currentslot)

    def delete(self):
        self.rp.delete(self.currentslot)

    def moveToRid(self, rid):
        self.close()
        blk = BlockId(self.filename, rid.blockNumber())
        self.rp = RecordPage(self.tx, blk, self.layout)
        self.currentslot = rid.slot()

    def getRid(self):
        return RID(self.rp.block().number(), self.currentslot)

    #  Private auxiliary methods
    def moveToBlock(self, blknum):
        self.close()
        blk = BlockId(self.filename, blknum)
        self.rp = RecordPage(self.tx, blk, self.layout)
        self.currentslot = -1

    def moveToNewBlock(self):
        self.close()
        blk = self.tx.append(self.filename)
        self.rp = RecordPage(self.tx, blk, self.layout)
        self.rp.format()
        self.currentslot = -1

    def atLastBlock(self):
        return self.rp.block().number() == self.tx.size(self.filename) - 1
