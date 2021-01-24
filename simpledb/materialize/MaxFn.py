#
#  * The <i>max</i> aggregation function.
#  * @author Edward Sciore
#
from simpledb.materialize.AggregationFn import AggregationFn


class MaxFn(AggregationFn):

    #
    #     * Create a max aggregation function for the specified field.
    #     * @param fldname the name of the aggregated field
    #
    def __init__(self, fldname):
        super(MaxFn, self).__init__()
        self.fldname = fldname

    #
    #     * Start a new maximum to be the
    #     * field value in the current record.
    #     * @see simpledb.materialize.AggregationFn#processFirst(Scan)
    #
    def processFirst(self, s):
        self.val = s.getVal(self.fldname)

    #
    #     * Replace the current maximum by the field value
    #     * in the current record, if it is higher.
    #     * @see simpledb.materialize.AggregationFn#processNext(Scan)
    #
    def processNext(self, s):
        newval = s.getVal(self.fldname)
        if newval.__cmp__(self.val) > 0:
            self.val = newval

    #
    #     * Return the field's name, prepended by "maxof".
    #     * @see simpledb.materialize.AggregationFn#fieldName()
    #
    def fieldName(self):
        return "maxof" + self.fldname

    #
    #     * Return the current maximum.
    #     * @see simpledb.materialize.AggregationFn#value()
    #
    def value(self):
        return self.val
