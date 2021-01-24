#
#  * An object that holds the values of the grouping fields
#  * for the current record of a scan.
#  * @author Edward Sciore
#


class GroupValue(object):

    #
    #     * Create a new group value, given the specified scan
    #     * and list of fields.
    #     * The values in the current record of each field are
    #     * stored.
    #     * @param s a scan
    #     * @param fields the list of fields
    #
    def __init__(self, s, fields):
        self.vals = {}
        for fldname in fields:
            self.vals[fldname] = s.getVal(fldname)

    #
    #     * Return the Constant value of the specified field in the group.
    #     * @param fldname the name of a field
    #     * @return the value of the field in the group
    #
    def getVal(self, fldname):
        return self.vals.get(fldname)

    #
    #     * Two GroupValue objects are equal if they have the same values
    #     * for their grouping fields.
    #     * @see Object#equals(Object)
    #
    def __eq__(self, obj):
        gv = obj
        for fldname in self.vals.keys():
            v1 = self.vals.get(fldname)
            v2 = gv.getVal(fldname)
            if not v1 == v2:
                return False
        return True

    #
    #     * The hashcode of a GroupValue object is the sum of the
    #     * hashcodes of its field values.
    #     * @see Object#hashCode()
    #
    def __hash__(self):
        hashval = 0
        for c in self.vals.values():
            hashval += c.hashCode()
        return hashval
