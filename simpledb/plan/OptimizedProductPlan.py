#  A Plan class corresponding to the <i>product</i>
#   * relational algebra operator that determines the
#   * most efficient ordering of its inputs.
#   * @author Edward Sciore
#
from simpledb.plan.Plan import Plan
from simpledb.plan.ProductPlan import ProductPlan


class OptimizedProductPlan(Plan):
    bestplan = Plan()

    def __init__(self, p1, p2):
        super(OptimizedProductPlan, self).__init__()
        prod1 = ProductPlan(p1, p2)
        prod2 = ProductPlan(p2, p1)
        b1 = prod1.blocksAccessed()
        b2 = prod2.blocksAccessed()
        self.bestplan = prod1 if (b1 < b2) else prod2

    def open(self):
        return self.bestplan.open()

    def blocksAccessed(self):
        return self.bestplan.blocksAccessed()

    def recordsOutput(self):
        return self.bestplan.recordsOutput()

    def distinctValues(self, fldname):
        return self.bestplan.distinctValues(fldname)

    def schema(self):
        return self.bestplan.schema()
