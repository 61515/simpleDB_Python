#
#  * The Plan class for the <i>mergejoin</i> operator.
#  * @author Edward Sciore
#
from simpledb.materialize.MergeJoinScan import MergeJoinScan
from simpledb.materialize.SortPlan import SortPlan
from simpledb.plan.Plan import Plan
from simpledb.record.Schema import Schema


class MergeJoinPlan(Plan):

    #
    #     * Creates a mergejoin plan for the two specified queries.
    #     * The RHS must be materialized after it is sorted,
    #     * in order to deal with possible duplicates.
    #     * @param p1 the LHS query plan
    #     * @param p2 the RHS query plan
    #     * @param fldname1 the LHS join field
    #     * @param fldname2 the RHS join field
    #     * @param tx the calling transaction
    #
    def __init__(self, tx, p1, p2, fldname1, fldname2):
        super(MergeJoinPlan, self).__init__()
        self.fldname1 = fldname1
        sortlist1 = [fldname1]
        self.p1 = SortPlan(tx, p1, sortlist1)

        self.fldname2 = fldname2
        sortlist2 = [fldname2]
        self.p2 = SortPlan(tx, p2, sortlist2)

        self.sch = Schema()
        self.sch.addAll(p1.schema())
        self.sch.addAll(p2.schema())

    #  The method first sorts its two underlying scans
    #      * on their join field. It then returns a mergejoin scan
    #      * of the two sorted table scans.
    #      * @see Plan#open()
    #
    def open(self):
        s1 = self.p1.open()
        s2 = self.p2.open()
        return MergeJoinScan(s1, s2, self.fldname1, self.fldname2)

    #
    #     * Return the number of block acceses required to
    #     * mergejoin the sorted tables.
    #     * Since a mergejoin can be preformed with a single
    #     * pass through each table, the method returns
    #     * the sum of the block accesses of the
    #     * materialized sorted tables.
    #     * It does <i>not</i> include the one-time cost
    #     * of materializing and sorting the records.
    #     * @see Plan#blocksAccessed()
    #
    def blocksAccessed(self):
        return self.p1.blocksAccessed() + self.p2.blocksAccessed()

    #
    #     * Return the number of records in the join.
    #     * Assuming uniform distribution, the formula is:
    #     * <pre> R(join(p1,p2)) = R(p1)*R(p2)/max{V(p1,F1),V(p2,F2)}</pre>
    #     * @see Plan#recordsOutput()
    #
    def recordsOutput(self):
        maxvals = max(self.p1.distinctValues(self.fldname1), self.p2.distinctValues(self.fldname2))
        return int((self.p1.recordsOutput() * self.p2.recordsOutput()) / maxvals)

    #
    #     * Estimate the distinct number of field values in the join.
    #     * Since the join does not increase or decrease field values,
    #     * the estimate is the same as in the appropriate underlying query.
    #     * @see Plan#distinctValues(String)
    #
    def distinctValues(self, fldname):
        if self.p1.schema().hasField(fldname):
            return self.p1.distinctValues(fldname)
        else:
            return self.p2.distinctValues(fldname)

    #
    #     * Return the schema of the join,
    #     * which is the union of the schemas of the underlying queries.
    #     * @see Plan#schema()
    #
    def schema(self):
        return self.sch
