#
#  * The scan class corresponding to the indexjoin relational
#  * algebra operator.
#  * The code is very similar to that of ProductScan,
#  * which makes sense because an index join is essentially
#  * the product of each LHS record with the matching RHS index records.
#  * @author Edward Sciore
#
from simpledb.query.Scan import Scan


class IndexJoinScan(Scan):

    #
    #     * Creates an index join scan for the specified LHS scan and
    #     * RHS index.
    #     * @param lhs the LHS scan
    #     * @param idx the RHS index
    #     * @param joinfield the LHS field used for joining
    #     * @param rhs the RHS scan
    #
    def __init__(self, lhs, idx, joinfield, rhs):
        super(IndexJoinScan, self).__init__()
        self.lhs = lhs
        self.idx = idx
        self.joinfield = joinfield
        self.rhs = rhs
        self.beforeFirst()

    #
    #     * Positions the scan before the first record.
    #     * That is, the LHS scan will be positioned at its
    #     * first record, and the index will be positioned
    #     * before the first record for the join value.
    #     * @see simpledb.query.Scan#beforeFirst()
    #
    def beforeFirst(self):
        self.lhs.beforeFirst()
        self.lhs.next
        self.resetIndex()

    #
    #     * Moves the scan to the next record.
    #     * The method moves to the next index record, if possible.
    #     * Otherwise, it moves to the next LHS record and the
    #     * first index record.
    #     * If there are no more LHS records, the method returns false.
    #     * @see simpledb.query.Scan#next()
    #
    def next(self):
        while True:
            if self.idx.next:
                self.rhs.moveToRid(self.idx.getDataRid())
                return True
            if not self.lhs.next:
                return False
            self.resetIndex()

    #
    #     * Returns the integer value of the specified field.
    #     * @see simpledb.query.Scan#getVal(String)
    #
    def getInt(self, fldname):
        if self.rhs.hasField(fldname):
            return self.rhs.getInt(fldname)
        else:
            return self.lhs.getInt(fldname)

    #
    #     * Returns the Constant value of the specified field.
    #     * @see simpledb.query.Scan#getVal(String)
    #
    def getVal(self, fldname):
        if self.rhs.hasField(fldname):
            return self.rhs.getVal(fldname)
        else:
            return self.lhs.getVal(fldname)

    #
    #     * Returns the string value of the specified field.
    #     * @see simpledb.query.Scan#getVal(String)
    #
    def getString(self, fldname):
        if self.rhs.hasField(fldname):
            return self.rhs.getString(fldname)
        else:
            return self.lhs.getString(fldname)

    #  Returns true if the field is in the schema.
    #      * @see simpledb.query.Scan#hasField(String)
    #
    def hasField(self, fldname):
        return self.rhs.hasField(fldname) or self.lhs.hasField(fldname)

    #
    #     * Closes the scan by closing its LHS scan and its RHS index.
    #     * @see simpledb.query.Scan#close()
    #
    def close(self):
        self.lhs.close()
        self.idx.close()
        self.rhs.close()

    def resetIndex(self):
        searchkey = self.lhs.getVal(self.joinfield)
        self.idx.beforeFirst(searchkey)
