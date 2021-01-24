#
#  * The scan class corresponding to the <i>product</i> relational
#  * algebra operator.
#  * @author Edward Sciore
#
from simpledb.query.Scan import Scan


class ProductScan(Scan):

    #
    #     * Create a product scan having the two underlying scans.
    #     * @param s1 the LHS scan
    #     * @param s2 the RHS scan
    #
    def __init__(self, s1, s2):
        super(ProductScan, self).__init__()
        self.s1 = s1
        self.s2 = s2
        self.beforeFirst()

    #
    #     * Position the scan before its first record.
    #     * In particular, the LHS scan is positioned at
    #     * its first record, and the RHS scan
    #     * is positioned before its first record.
    #     * @see Scan#beforeFirst()
    #
    def beforeFirst(self):
        self.s1.beforeFirst()
        self.s1.next()
        self.s2.beforeFirst()

    #
    #     * Move the scan to the next record.
    #     * The method moves to the next RHS record, if possible.
    #     * Otherwise, it moves to the next LHS record and the
    #     * first RHS record.
    #     * If there are no more LHS records, the method returns false.
    #     * @see Scan#next()
    #
    def next(self):
        if self.s2.next():
            return True
        else:
            self.s2.beforeFirst()
            return self.s2.next() and self.s1.next()

    #
    #     * Return the integer value of the specified field.
    #     * The value is obtained from whichever scan
    #     * contains the field.
    #     * @see Scan#getInt(String)
    #
    def getInt(self, fldname):
        if self.s1.hasField(fldname):
            return self.s1.getInt(fldname)
        else:
            return self.s2.getInt(fldname)

    #
    #     * Returns the string value of the specified field.
    #     * The value is obtained from whichever scan
    #     * contains the field.
    #     * @see Scan#getString(String)
    #
    def getString(self, fldname):
        if self.s1.hasField(fldname):
            return self.s1.getString(fldname)
        else:
            return self.s2.getString(fldname)

    #
    #     * Return the value of the specified field.
    #     * The value is obtained from whichever scan
    #     * contains the field.
    #     * @see Scan#getVal(String)
    #
    def getVal(self, fldname):
        if self.s1.hasField(fldname):
            return self.s1.getVal(fldname)
        else:
            return self.s2.getVal(fldname)

    #
    #     * Returns true if the specified field is in
    #     * either of the underlying scans.
    #     * @see Scan#hasField(String)
    #
    def hasField(self, fldname):
        return self.s1.hasField(fldname) or self.s2.hasField(fldname)

    #
    #     * Close both underlying scans.
    #     * @see Scan#close()
    #
    def close(self):
        self.s1.close()
        self.s2.close()
