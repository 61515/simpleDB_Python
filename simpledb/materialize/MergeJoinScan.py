#
#  * The Scan class for the <i>mergejoin</i> operator.
#  * @author Edward Sciore
#
from simpledb.query.Scan import Scan


class MergeJoinScan(Scan):

    #
    #     * Create a mergejoin scan for the two underlying sorted scans.
    #     * @param s1 the LHS sorted scan
    #     * @param s2 the RHS sorted scan
    #     * @param fldname1 the LHS join field
    #     * @param fldname2 the RHS join field
    #
    def __init__(self, s1, s2, fldname1, fldname2):
        super(MergeJoinScan, self).__init__()
        self.s1 = s1
        self.s2 = s2
        self.fldname1 = fldname1
        self.fldname2 = fldname2
        self.joinval = None
        self.beforeFirst()

    #
    #     * Close the scan by closing the two underlying scans.
    #     * @see Scan#close()
    #
    def close(self):
        """ generated source for method close """
        self.s1.close()
        self.s2.close()

    #
    #     * Position the scan before the first record,
    #     * by positioning each underlying scan before
    #     * their first records.
    #     * @see Scan#beforeFirst()
    #
    def beforeFirst(self):
        self.s1.beforeFirst()
        self.s2.beforeFirst()

    #
    #     * Move to the next record.  This is where the action is.
    #     * <P>
    #     * If the next RHS record has the same join value,
    #     * then move to it.
    #     * Otherwise, if the next LHS record has the same join value,
    #     * then reposition the RHS scan back to the first record
    #     * having that join value.
    #     * Otherwise, repeatedly move the scan having the smallest
    #     * value until a common join value is found.
    #     * When one of the scans runs out of records, return false.
    #     * @see Scan#next()
    #
    def next(self):
        hasmore2 = self.s2.next()
        if hasmore2 and self.s2.getVal(self.fldname2) == self.joinval:
            return True

        hasmore1 = self.s1.next()
        if hasmore1 and self.s1.getVal(self.fldname1) == self.joinval:
            self.s2.restorePosition()
            return True

        while hasmore1 and hasmore2:
            v1 = self.s1.getVal(self.fldname1)
            v2 = self.s2.getVal(self.fldname2)
            if v1.compareTo(v2) < 0:
                hasmore1 = self.s1.next()
            elif v1.compareTo(v2) > 0:
                hasmore2 = self.s2.next()
            else:
                self.s2.savePosition()
                self.joinval = self.s2.getVal(self.fldname2)
                return True
        return False

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
    #     * Return the string value of the specified field.
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
    #     * Return true if the specified field is in
    #     * either of the underlying scans.
    #     * @see Scan#hasField(String)
    #
    def hasField(self, fldname):
        return self.s1.hasField(fldname) or self.s2.hasField(fldname)
