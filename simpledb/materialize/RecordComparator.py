#
#  * A comparator for scans.
#  * @author Edward Sciore
#


class RecordComparator:

    #
    #     * Create a comparator using the specified fields,
    #     * using the ordering implied by its iterator.
    #     * @param fields a list of field names
    #
    def __init__(self, fields):
        super(RecordComparator, self).__init__()
        self.fields = fields

    #
    #     * Compare the current records of the two specified scans.
    #     * The sort fields are considered in turn.
    #     * When a field is encountered for which the records have
    #     * different values, those values are used as the result
    #     * of the comparison.
    #     * If the two records have the same values for all
    #     * sort fields, then the method returns 0.
    #     * @param s1 the first scan
    #     * @param s2 the second scan
    #     * @return the result of comparing each scan's current record according to the field list
    #
    def compare(self, s1, s2):
        for fldname in self.fields:
            val1 = s1.getVal(fldname)
            val2 = s2.getVal(fldname)
            result = val1.__cmp__(val2)
            if result != 0:
                return result
        return 0
