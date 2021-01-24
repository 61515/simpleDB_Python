#
#  * The Scan class for the <i>groupby</i> operator.
#  * @author Edward Sciore
#
from simpledb.materialize.GroupValue import GroupValue
from simpledb.query.Scan import Scan


class GroupByScan(Scan):

    #
    #     * Create a groupby scan, given a grouped table scan.
    #     * @param s the grouped scan
    #     * @param groupfields the group fields
    #     * @param aggfns the aggregation functions
    #
    def __init__(self, s, groupfields, aggfns):
        super(GroupByScan, self).__init__()
        self.s = s
        self.groupfields = groupfields
        self.aggfns = aggfns
        self.beforeFirst()

    #
    #     * Position the scan before the first group.
    #     * Internally, the underlying scan is always
    #     * positioned at the first record of a group, which
    #     * means that this method moves to the
    #     * first underlying record.
    #     * @see Scan#beforeFirst()
    #
    def beforeFirst(self):
        self.s.beforeFirst()
        self.moregroups = self.s.next

    #
    #     * Move to the next group.
    #     * The key of the group is determined by the
    #     * group values at the current record.
    #     * The method repeatedly reads underlying records until
    #     * it encounters a record having a different key.
    #     * The aggregation functions are called for each record
    #     * in the group.
    #     * The values of the grouping fields for the group are saved.
    #     * @see Scan#next()
    #
    def next(self):
        if not self.moregroups:
            return False
        for fn in self.aggfns:
            fn.processFirst(self.s)
        self.groupval = GroupValue(self.s, self.groupfields)
        while True:
            self.moregroups = self.s.next()
            if not self.moregroups:
                break

            gv = GroupValue(self.s, self.groupfields)
            if not self.groupval == gv:
                break
            for fn in self.aggfns:
                fn.processNext(self.s)
        return True

    #
    #     * Close the scan by closing the underlying scan.
    #     * @see Scan#close()
    #
    def close(self):
        self.s.close()

    #
    #     * Get the Constant value of the specified field.
    #     * If the field is a group field, then its value can
    #     * be obtained from the saved group value.
    #     * Otherwise, the value is obtained from the
    #     * appropriate aggregation function.
    #     * @see Scan#getVal(String)
    #
    def getVal(self, fldname):
        if self.groupfields.contains(fldname):
            return self.groupval.getVal(fldname)
        for fn in self.aggfns:
            if fn.fieldName() == fldname:
                return fn.value()
        raise RuntimeError("field " + fldname + " not found.")

    #
    #     * Get the integer value of the specified field.
    #     * If the field is a group field, then its value can
    #     * be obtained from the saved group value.
    #     * Otherwise, the value is obtained from the
    #     * appropriate aggregation function.
    #     * @see Scan#getVal(String)
    #
    def getInt(self, fldname):
        return self.getVal(fldname).asInt()

    #
    #     * Get the string value of the specified field.
    #     * If the field is a group field, then its value can
    #     * be obtained from the saved group value.
    #     * Otherwise, the value is obtained from the
    #     * appropriate aggregation function.
    #     * @see Scan#getVal(String)
    #
    def getString(self, fldname):
        return self.getVal(fldname).asString()

    #  Return true if the specified field is either a
    #     * grouping field or created by an aggregation function.
    #     * @see Scan#hasField(String)
    #
    def hasField(self, fldname):
        if self.groupfields.contains(fldname):
            return True
        for fn in self.aggfns:
            if fn.fieldName() == fldname:
                return True
        return False
