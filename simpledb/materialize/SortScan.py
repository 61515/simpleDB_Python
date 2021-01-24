#
#  * The Scan class for the <i>sort</i> operator.
#  * @author Edward Sciore
#
#
#  * @author sciore
#  *
#
from simpledb.query.Scan import Scan


class SortScan(Scan):

    #
    #     * Create a sort scan, given a list of 1 or 2 runs.
    #     * If there is only 1 run, then s2 will be null and
    #     * hasmore2 will be false.
    #     * @param runs the list of runs
    #     * @param comp the record comparator
    #
    def __init__(self, runs, comp):
        super(SortScan, self).__init__()
        self.savedposition = None
        self.comp = comp
        self.s1 = runs[0].open()
        self.hasmore1 = self.s1.next
        if len(runs) > 1:
            self.s2 = runs[1].open()
            self.hasmore2 = self.s2.next

    #
    #     * Position the scan before the first record in sorted order.
    #     * Internally, it moves to the first record of each underlying scan.
    #     * The variable currentscan is set to null, indicating that there is
    #     * no current scan.
    #     * @see Scan#beforeFirst()
    #
    def beforeFirst(self):
        self.currentscan = None
        self.s1.beforeFirst()
        self.hasmore1 = self.s1.next
        if self.s2 is not None:
            self.s2.beforeFirst()
            self.hasmore2 = self.s2.next

    #
    #     * Move to the next record in sorted order.
    #     * First, the current scan is moved to the next record.
    #     * Then the lowest record of the two scans is found, and that
    #     * scan is chosen to be the new current scan.
    #     * @see Scan#next()
    #
    def next(self):
        if self.currentscan is not None:
            if self.currentscan == self.s1:
                self.hasmore1 = self.s1.next
            elif self.currentscan == self.s2:
                self.hasmore2 = self.s2.next

        if not self.hasmore1 and not self.hasmore2:
            return False
        elif self.hasmore1 and self.hasmore2:
            if self.comp.compare(self.s1, self.s2) < 0:
                self.currentscan = self.s1
            else:
                self.currentscan = self.s2
        elif self.hasmore1:
            self.currentscan = self.s1
        elif self.hasmore2:
            self.currentscan = self.s2
        return True

    #
    #     * Close the two underlying scans.
    #     * @see Scan#close()
    #
    def close(self):
        self.s1.close()
        if self.s2 is not None:
            self.s2.close()

    #
    #     * Get the Constant value of the specified field
    #     * of the current scan.
    #     * @see Scan#getVal(String)
    #
    def getVal(self, fldname):
        return self.currentscan.getVal(fldname)

    #
    #     * Get the integer value of the specified field
    #     * of the current scan.
    #     * @see Scan#getInt(String)
    #
    def getInt(self, fldname):
        return self.currentscan.getInt(fldname)

    #
    #     * Get the string value of the specified field
    #     * of the current scan.
    #     * @see Scan#getString(String)
    #
    def getString(self, fldname):
        return self.currentscan.getString(fldname)

    #
    #     * Return true if the specified field is in the current scan.
    #     * @see Scan#hasField(String)
    #
    def hasField(self, fldname):
        return self.currentscan.hasField(fldname)

    #
    #     * Save the position of the current record,
    #     * so that it can be restored at a later time.
    #
    def savePosition(self):
        rid1 = self.s1.getRid()
        rid2 = None if (self.s2 is None) else self.s2.getRid()
        self.saveposition = [rid1, rid2]

    #
    #     * Move the scan to its previously-saved position.
    #
    def restorePosition(self):
        rid1 = self.savedposition[0]
        rid2 = self.savedposition[1]
        self.s1.moveToRid(rid1)
        if rid2 is not None:
            self.s2.moveToRid(rid2)
