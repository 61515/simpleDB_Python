#
#  * The Plan class for the multi-buffer version of the
#  * <i>product</i> operator.
#  * @author Edward Sciore
#
from simpledb.materialize.MaterializePlan import MaterializePlan
from simpledb.materialize.TempTable import TempTable
from simpledb.multibuffer.MultibufferProductScan import MultibufferProductScan
from simpledb.plan.Plan import Plan
from simpledb.record.Schema import Schema


class MultibufferProductPlan(Plan):

    #
    #     * Creates a product plan for the specified queries.
    #     * @param lhs the plan for the LHS query
    #     * @param rhs the plan for the RHS query
    #     * @param tx the calling transaction
    #
    def __init__(self, tx, lhs, rhs):
        super(MultibufferProductPlan, self).__init__()
        self.tx = tx
        self.lhs = MaterializePlan(tx, lhs)
        self.rhs = rhs

        self.schema = Schema()
        self.schema.addAll(lhs.schema())
        self.schema.addAll(rhs.schema())

    #
    #     * A scan for this query is created and returned, as follows.
    #     * First, the method materializes its LHS and RHS queries.
    #     * It then determines the optimal chunk size,
    #     * based on the size of the materialized RHS file and the
    #     * number of available buffers.
    #     * It creates a chunk plan for each chunk, saving them in a list.
    #     * Finally, it creates a multiscan for this list of plans,
    #     * and returns that scan.
    #     * @see Plan#open()
    #
    def open(self):
        leftscan = self.lhs.open()
        tt = self.copyRecordsFrom(self.rhs)
        return MultibufferProductScan(self.tx, leftscan, tt.tableName(), tt.getLayout())

    #
    #     * Returns an estimate of the number of block accesses
    #     * required to execute the query. The formula is:
    #     * <pre> B(product(p1,p2)) = B(p2) + B(p1)*C(p2) </pre>
    #     * where C(p2) is the number of chunks of p2.
    #     * The method uses the current number of available buffers
    #     * to calculate C(p2), and so this value may differ
    #     * when the query scan is opened.
    #     * @see Plan#blocksAccessed()
    #
    def blocksAccessed(self):
        #  this guesses at the # of chunks
        avail = self.tx.availableBuffs()
        size = MaterializePlan(self.tx, self.rhs).blocksAccessed()
        numchunks = int(size / avail)
        return self.rhs.blocksAccessed() + (self.lhs.blocksAccessed() * numchunks)

    #
    #     * Estimates the number of output records in the product.
    #     * The formula is:
    #     * <pre> R(product(p1,p2)) = R(p1)*R(p2) </pre>
    #     * @see Plan#recordsOutput()
    #
    def recordsOutput(self):
        return self.lhs.recordsOutput() * self.rhs.recordsOutput()

    #
    #     * Estimates the distinct number of field values in the product.
    #     * Since the product does not increase or decrease field values,
    #     * the estimate is the same as in the appropriate underlying query.
    #     * @see Plan#distinctValues(String)
    #
    def distinctValues(self, fldname):
        if self.lhs.schema().hasField(fldname):
            return self.lhs.distinctValues(fldname)
        else:
            return self.rhs.distinctValues(fldname)

    #
    #     * Returns the schema of the product,
    #     * which is the union of the schemas of the underlying queries.
    #     * @see Plan#schema()
    #
    def schema(self):
        return self.schema

    def copyRecordsFrom(self, p):
        src = p.open()
        sch = p.schema()
        t = TempTable(self.tx, sch)
        dest = t.open()
        while src.next():
            dest.insert()
            for fldname in sch.fields():
                dest.setVal(fldname, src.getVal(fldname))
        src.close()
        dest.close()
        return t
