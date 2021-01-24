#
#  * The Plan class for the <i>groupby</i> operator.
#  * @author Edward Sciore
#
from simpledb.materialize.GroupByScan import GroupByScan
from simpledb.materialize.SortPlan import SortPlan
from simpledb.plan.Plan import Plan
from simpledb.record.Schema import Schema


class GroupByPlan(Plan):

    #
    #     * Create a groupby plan for the underlying query.
    #     * The grouping is determined by the specified
    #     * collection of group fields,
    #     * and the aggregation is computed by the
    #     * specified collection of aggregation functions.
    #     * @param p a plan for the underlying query
    #     * @param groupfields the group fields
    #     * @param aggfns the aggregation functions
    #     * @param tx the calling transaction
    #
    def __init__(self, tx, p, groupfields, aggfns):
        super(GroupByPlan, self).__init__()
        self.p = SortPlan(tx, p, groupfields)
        self.groupfields = groupfields
        self.aggfns = aggfns
        self.sch = Schema()
        for fldname in groupfields:
            self.sch.add(fldname, p.schema())
        for fn in aggfns:
            self.sch.addIntField(fn.fieldName())

    #
    #     * This method opens a sort plan for the specified plan.
    #     * The sort plan ensures that the underlying records
    #     * will be appropriately grouped.
    #     * @see Plan#open()
    #
    def open(self):
        s = self.p.open()
        return GroupByScan(s, self.groupfields, self.aggfns)

    #
    #     * Return the number of blocks required to
    #     * compute the aggregation,
    #     * which is one pass through the sorted table.
    #     * It does <i>not</i> include the one-time cost
    #     * of materializing and sorting the records.
    #     * @see Plan#blocksAccessed()
    #
    def blocksAccessed(self):
        return self.p.blocksAccessed()

    #
    #     * Return the number of groups.  Assuming equal distribution,
    #     * this is the product of the distinct values
    #     * for each grouping field.
    #     * @see Plan#recordsOutput()
    #
    def recordsOutput(self):
        numgroups = 1
        for fldname in self.groupfields:
            numgroups *= self.p.distinctValues(fldname)
        return numgroups

    #
    #     * Return the number of distinct values for the
    #     * specified field.  If the field is a grouping field,
    #     * then the number of distinct values is the same
    #     * as in the underlying query.
    #     * If the field is an aggregate field, then we
    #     * assume that all values are distinct.
    #     * @see Plan#distinctValues(String)
    #
    def distinctValues(self, fldname):
        if self.p.schema().hasField(fldname):
            return self.p.distinctValues(fldname)
        else:
            return self.recordsOutput()

    #
    #     * Returns the schema of the output table.
    #     * The schema consists of the group fields,
    #     * plus one field for each aggregation function.
    #     * @see Plan#schema()
    #
    def schema(self):
        return self.sch
