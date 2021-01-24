#
#  * The scan class corresponding to the <i>select</i> relational
#  * algebra operator.
#  * All methods except next delegate their work to the
#  * underlying scan.
#  * @author Edward Sciore
#
from simpledb.query.UpdateScan import UpdateScan


class SelectScan(UpdateScan):

    #
    #    * Create a select scan having the specified underlying
    #    * scan and predicate.
    #    * @param s the scan of the underlying query
    #    * @param pred the selection predicate
    #
    def __init__(self, s, pred):
        super(SelectScan, self).__init__()
        self.s = s
        self.pred = pred

    #  Scan methods

    def beforeFirst(self):
        self.s.beforeFirst()

    def next(self):
        while self.s.next():
            if self.pred.isSatisfied(self.s):
                return True
        return False

    def getInt(self, fldname):
        return self.s.getInt(fldname)

    def getString(self, fldname):
        return self.s.getString(fldname)

    def getVal(self, fldname):
        return self.s.getVal(fldname)

    def hasField(self, fldname):
        return self.s.hasField(fldname)

    def close(self):
        self.s.close()

    #  UpdateScan methods

    def setInt(self, fldname, val):
        us = self.s
        us.setInt(fldname, val)

    def setString(self, fldname, val):
        us = self.s
        us.setString(fldname, val)

    def setVal(self, fldname, val):
        us = self.s
        us.setVal(fldname, val)

    def delete(self):
        us = self.s
        us.delete()

    def insert(self):
        us = self.s
        us.insert()

    def getRid(self):
        us = self.s
        return us.getRid()

    def moveToRid(self, rid):
        us = self.s
        us.moveToRid(rid)
