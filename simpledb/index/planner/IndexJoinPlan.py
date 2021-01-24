#  The Plan class corresponding to the <i>indexjoin</i>
#   * relational algebra operator.
#   * @author Edward Sciore
#
from simpledb.index.query.IndexJoinScan import IndexJoinScan
from simpledb.plan.Plan import Plan
from simpledb.record.Schema import Schema


class IndexJoinPlan(Plan):

    #
    #     * Implements the join operator,
    #     * using the specified LHS and RHS plans.
    #     * @param p1 the left-hand plan
    #     * @param p2 the right-hand plan
    #     * @param ii information about the right-hand index
    #     * @param joinfield the left-hand field used for joining
    #
    def __init__(self, p1, p2, ii, joinfield):
        super(IndexJoinPlan, self).__init__()
        self.p1 = p1
        self.p2 = p2
        self.ii = ii
        self.joinfield = joinfield
        self.sch = Schema()
        self.sch.addAll(p1.schema())
        self.sch.addAll(p2.schema())

    #
    #     * Opens an indexjoin scan for this query
    #     * @see Plan#open()
    #
    def open(self):
        s = self.p1.open()
        #  throws an exception if p2 is not a tableplan
        ts = self.p2.open()
        idx = self.ii.open()
        return IndexJoinScan(s, idx, self.joinfield, ts)

    #
    #     * Estimates the number of block accesses to compute the join.
    #     * The formula is:
    #     * <pre> B(indexjoin(p1,p2,idx)) = B(p1) + R(p1)*B(idx)
    #     *       + R(indexjoin(p1,p2,idx) </pre>
    #     * @see Plan#blocksAccessed()
    #
    def blocksAccessed(self):
        return self.p1.blocksAccessed() + (self.p1.recordsOutput() * self.ii.blocksAccessed()) + self.recordsOutput()

    #
    #     * Estimates the number of output records in the join.
    #     * The formula is:
    #     * <pre> R(indexjoin(p1,p2,idx)) = R(p1)*R(idx) </pre>
    #     * @see Plan#recordsOutput()
    #
    def recordsOutput(self):
        return self.p1.recordsOutput() * self.ii.recordsOutput()

    #
    #     * Estimates the number of distinct values for the
    #     * specified field.
    #     * @see Plan#distinctValues(String)
    #
    def distinctValues(self, fldname):
        if self.p1.schema().hasField(fldname):
            return self.p1.distinctValues(fldname)
        else:
            return self.p2.distinctValues(fldname)

    #
    #     * Returns the schema of the index join.
    #     * @see Plan#schema()
    #
    def schema(self):
        return self.sch
