#
#  * The <i>count</i> aggregation function.
#  * @author Edward Sciore
#
from simpledb.materialize.AggregationFn import AggregationFn
from simpledb.query.Constant import Constant


class CountFn(AggregationFn):

    #
    #     * Create a count aggregation function for the specified field.
    #     * @param fldname the name of the aggregated field
    #
    def __init__(self, fldname):
        super(CountFn, self).__init__()
        self.fldname = fldname

    #
    #     * Start a new count.
    #     * Since SimpleDB does not support null values,
    #     * every record will be counted,
    #     * regardless of the field.
    #     * The current count is thus set to 1.
    #     * @see simpledb.materialize.AggregationFn#processFirst(Scan)
    #
    def processFirst(self, s):
        self.count = 1

    #
    #     * Since SimpleDB does not support null values,
    #     * this method always increments the count,
    #     * regardless of the field.
    #     * @see simpledb.materialize.AggregationFn#processNext(Scan)
    #
    def processNext(self, s):
        self.count += 1

    #
    #     * Return the field's name, prepended by "countof".
    #     * @see simpledb.materialize.AggregationFn#fieldName()
    #
    def fieldName(self):
        return "countof" + self.fldname

    #
    #     * Return the current count.
    #     * @see simpledb.materialize.AggregationFn#value()
    #
    def value(self):
        return Constant(self.count)
