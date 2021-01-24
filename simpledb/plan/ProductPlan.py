
#  The Plan class corresponding to the <i>product</i>
#   * relational algebra operator.
#   * @author Edward Sciore
#
from simpledb.plan.Plan import Plan
from simpledb.query.ProductScan import ProductScan
from simpledb.record.Schema import Schema


class ProductPlan(Plan):

    #
    #     * Creates a new product node in the query tree,
    #     * having the two specified subqueries.
    #     * @param p1 the left-hand subquery
    #     * @param p2 the right-hand subquery
    #
    def __init__(self, p1, p2):
        super(ProductPlan, self).__init__()
        self.p1 = p1
        self.p2 = p2
        self._schema = Schema()
        self._schema.addAll(p1.schema())
        self._schema.addAll(p2.schema())

    #
    #     * Creates a product scan for this query.
    #     * @see Plan#open()
    #
    def open(self):
        s1 = self.p1.open()
        s2 = self.p2.open()
        return ProductScan(s1, s2)

    #
    #     * Estimates the number of block accesses in the product.
    #     * The formula is:
    #     * <pre> B(product(p1,p2)) = B(p1) + R(p1)*B(p2) </pre>
    #     * @see Plan#blocksAccessed()
    #
    def blocksAccessed(self):
        return self.p1.blocksAccessed() + (self.p1.recordsOutput() * self.p2.blocksAccessed())

    #
    #     * Estimates the number of output records in the product.
    #     * The formula is:
    #     * <pre> R(product(p1,p2)) = R(p1)*R(p2) </pre>
    #     * @see Plan#recordsOutput()
    #
    def recordsOutput(self):
        return self.p1.recordsOutput() * self.p2.recordsOutput()

    #
    #     * Estimates the distinct number of field values in the product.
    #     * Since the product does not increase or decrease field values,
    #     * the estimate is the same as in the appropriate underlying query.
    #     * @see Plan#distinctValues(String)
    #
    def distinctValues(self, fldname):
        if self.p1.schema().hasField(fldname):
            return self.p1.distinctValues(fldname)
        else:
            return self.p2.distinctValues(fldname)

    #
    #     * Returns the schema of the product,
    #     * which is the union of the schemas of the underlying queries.
    #     * @see Plan#schema()
    #
    def schema(self):
        return self._schema
