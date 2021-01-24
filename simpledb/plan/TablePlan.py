
#  The Plan class corresponding to a table.
#   * @author Edward Sciore
#
from simpledb.plan.Plan import Plan
from simpledb.record.TableScan import TableScan


class TablePlan(Plan):

    #
    #     * Creates a leaf node in the query tree corresponding
    #     * to the specified table.
    #     * @param tblname the name of the table
    #     * @param tx the calling transaction
    #
    def __init__(self, tx, tblname, md):
        super(TablePlan, self).__init__()
        self.tblname = tblname
        self.tx = tx
        self.layout = md.getLayout(tblname, tx)
        self.si = md.getStatInfo(tblname, self.layout, tx)

    #
    #     * Creates a table scan for this query.
    #     * @see Plan#open()
    #
    def open(self):
        return TableScan(self.tx, self.tblname, self.layout)

    #
    #     * Estimates the number of block accesses for the table,
    #     * which is obtainable from the statistics manager.
    #     * @see Plan#blocksAccessed()
    #
    def blocksAccessed(self):
        return self.si.blocksAccessed()

    #
    #     * Estimates the number of records in the table,
    #     * which is obtainable from the statistics manager.
    #     * @see Plan#recordsOutput()
    #
    def recordsOutput(self):
        return self.si.recordsOutput()

    #
    #     * Estimates the number of distinct field values in the table,
    #     * which is obtainable from the statistics manager.
    #     * @see Plan#distinctValues(String)
    #
    def distinctValues(self, fldname):
        return self.si.distinctValues(fldname)

    #
    #     * Determines the schema of the table,
    #     * which is obtainable from the catalog manager.
    #     * @see Plan#schema()
    #
    def schema(self):
        return self.layout.schema()
