#  The Plan class corresponding to the <i>select</i>
#   * relational algebra operator.
#   * @author Edward Sciore
#
from simpledb.plan.Plan import Plan
from simpledb.query.SelectScan import SelectScan


class SelectPlan(Plan):

    #
    #     * Creates a new select node in the query tree,
    #     * having the specified subquery and predicate.
    #     * @param p the subquery
    #     * @param pred the predicate
    #
    def __init__(self, p, pred):
        super(SelectPlan, self).__init__()
        self.p = p
        self.pred = pred

    #
    #     * Creates a select scan for this query.
    #     * @see Plan#open()
    #
    def open(self):
        s = self.p.open()
        return SelectScan(s, self.pred)

    #
    #     * Estimates the number of block accesses in the selection,
    #     * which is the same as in the underlying query.
    #     * @see Plan#blocksAccessed()
    #
    def blocksAccessed(self):
        return self.p.blocksAccessed()

    #
    #     * Estimates the number of output records in the selection,
    #     * which is determined by the
    #     * reduction factor of the predicate.
    #     * @see Plan#recordsOutput()
    #
    def recordsOutput(self):
        return self.p.recordsOutput() / self.pred.reductionFactor(self.p)

    #
    #     * Estimates the number of distinct field values
    #     * in the projection.
    #     * If the predicate contains a term equating the specified
    #     * field to a constant, then this value will be 1.
    #     * Otherwise, it will be the number of the distinct values
    #     * in the underlying query
    #     * (but not more than the size of the output table).
    #     * @see Plan#distinctValues(String)
    #
    def distinctValues(self, fldname):
        if self.pred.equatesWithConstant(fldname) is not None:
            return 1
        else:
            fldname2 = self.pred.equatesWithField(fldname)
            if fldname2 is not None:
                return min(self.p.distinctValues(fldname), self.p.distinctValues(fldname2))
            else:
                return self.p.distinctValues(fldname)

    #
    #     * Returns the schema of the selection,
    #     * which is the same as in the underlying query.
    #     * @see Plan#schema()
    #
    def schema(self):
        return self.p.schema()
