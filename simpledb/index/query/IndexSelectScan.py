
#
#  * The scan class corresponding to the select relational
#  * algebra operator.
#  * @author Edward Sciore
#
from simpledb.query.Scan import Scan


class IndexSelectScan(Scan):

    #
    #     * Creates an index select scan for the specified
    #     * index and selection constant.
    #     * @param idx the index
    #     * @param val the selection constant
    #
    def __init__(self, ts, idx, val):
        super(IndexSelectScan, self).__init__()
        self.ts = ts
        self.idx = idx
        self.val = val
        self.beforeFirst()

    #
    #     * Positions the scan before the first record,
    #     * which in this case means positioning the index
    #     * before the first instance of the selection constant.
    #     * @see simpledb.query.Scan#beforeFirst()
    #
    def beforeFirst(self):
        self.idx.beforeFirst(self.val)

    #
    #     * Moves to the next record, which in this case means
    #     * moving the index to the next record satisfying the
    #     * selection constant, and returning false if there are
    #     * no more such index records.
    #     * If there is a next record, the method moves the
    #     * tablescan to the corresponding data record.
    #     * @see simpledb.query.Scan#next()
    #
    def next(self):
        ok = self.idx.next
        if ok:
            rid = self.idx.getDataRid()
            self.ts.moveToRid(rid)
        return ok

    #
    #     * Returns the value of the field of the current data record.
    #     * @see simpledb.query.Scan#getInt(String)
    #
    def getInt(self, fldname):
        return self.ts.getInt(fldname)

    #
    #     * Returns the value of the field of the current data record.
    #     * @see simpledb.query.Scan#getString(String)
    #
    def getString(self, fldname):
        return self.ts.getString(fldname)

    #
    #     * Returns the value of the field of the current data record.
    #     * @see simpledb.query.Scan#getVal(String)
    #
    def getVal(self, fldname):
        return self.ts.getVal(fldname)

    #
    #     * Returns whether the data record has the specified field.
    #     * @see simpledb.query.Scan#hasField(String)
    #
    def hasField(self, fldname):
        return self.ts.hasField(fldname)

    #
    #     * Closes the scan by closing the index and the tablescan.
    #     * @see simpledb.query.Scan#close()
    #
    def close(self):
        self.idx.close()
        self.ts.close()
