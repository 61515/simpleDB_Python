#  The Plan class corresponding to the <i>indexselect</i>
#   * relational algebra operator.
#   * @author Edward Sciore
#
from simpledb.index.query.IndexSelectScan import IndexSelectScan
from simpledb.plan.Plan import Plan


class IndexSelectPlan(Plan):

    #
    #     * Creates a new indexselect node in the query tree
    #     * for the specified index and selection constant.
    #     * @param p the input table
    #     * @param ii information about the index
    #     * @param val the selection constant
    #     * @param tx the calling transaction
    #
    def __init__(self, p, ii, val):
        super(IndexSelectPlan, self).__init__()
        self.p = p
        self.ii = ii
        self.val = val

    #
    #     * Creates a new indexselect scan for this query
    #     * @see Plan#open()
    #
    def open(self):
        #  throws an exception if p is not a tableplan.
        ts = self.p.open()
        idx = self.ii.open()
        return IndexSelectScan(ts, idx, self.val)

    #
    #     * Estimates the number of block accesses to compute the
    #     * index selection, which is the same as the
    #     * index traversal cost plus the number of matching data records.
    #     * @see Plan#blocksAccessed()
    #
    def blocksAccessed(self):
        return self.ii.blocksAccessed() + self.recordsOutput()

    #
    #     * Estimates the number of output records in the index selection,
    #     * which is the same as the number of search key values
    #     * for the index.
    #     * @see Plan#recordsOutput()
    #
    def recordsOutput(self):
        return self.ii.recordsOutput()

    #
    #     * Returns the distinct values as defined by the index.
    #     * @see Plan#distinctValues(String)
    #
    def distinctValues(self, fldname):
        return self.ii.distinctValues(fldname)

    #
    #     * Returns the schema of the data table.
    #     * @see Plan#schema()
    #
    def schema(self):
        return self.p.schema()
