#
#  * The Plan class for the <i>materialize</i> operator.
#  * @author Edward Sciore
#
from simpledb.materialize.TempTable import TempTable
from simpledb.plan.Plan import Plan
from simpledb.record.Layout import Layout
import math


class MaterializePlan(Plan):

    #
    #     * Create a materialize plan for the specified query.
    #     * @param srcplan the plan of the underlying query
    #     * @param tx the calling transaction
    #
    def __init__(self, tx, srcplan):
        super(MaterializePlan, self).__init__()
        self.srcplan = srcplan
        self.tx = tx

    #
    #     * This method loops through the underlying query,
    #     * copying its output records into a temporary table.
    #     * It then returns a table scan for that table.
    #     * @see Plan#open()
    #
    def open(self):
        sch = self.srcplan.schema()
        temp = TempTable(self.tx, sch)
        src = self.srcplan.open()
        dest = temp.open()
        while src.next:
            dest.insert()
            for fldname in sch.fields():
                dest.setVal(fldname, src.getVal(fldname))
        src.close()
        dest.beforeFirst()
        return dest

    #
    #     * Return the estimated number of blocks in the
    #     * materialized table.
    #     * It does <i>not</i> include the one-time cost
    #     * of materializing the records.
    #     * @see Plan#blocksAccessed()
    #
    def blocksAccessed(self):
        #  create a dummy Layout object to calculate record length
        layout = Layout(self.srcplan.schema())
        rpb = float(self.tx.blockSize() / layout.slotSize())
        return math.ceil(self.srcplan.recordsOutput() / rpb)

    #
    #     * Return the number of records in the materialized table,
    #     * which is the same as in the underlying plan.
    #     * @see Plan#recordsOutput()
    #
    def recordsOutput(self):
        return self.srcplan.recordsOutput()

    #
    #     * Return the number of distinct field values,
    #     * which is the same as in the underlying plan.
    #     * @see Plan#distinctValues(String)
    #
    def distinctValues(self, fldname):
        return self.srcplan.distinctValues(fldname)

    #
    #     * Return the schema of the materialized table,
    #     * which is the same as in the underlying plan.
    #     * @see Plan#schema()
    #
    def schema(self):
        return self.srcplan.schema()
