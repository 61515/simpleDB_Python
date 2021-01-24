#
#  * A query planner that optimizes using a heuristic-based algorithm.
#  * @author Edward Sciore
#
from simpledb.opt.TablePlanner import TablePlanner
from simpledb.plan.ProjectPlan import ProjectPlan
from simpledb.plan.QueryPlanner import QueryPlanner


class HeuristicQueryPlanner(QueryPlanner):

    def __init__(self, mdm):
        super(HeuristicQueryPlanner, self).__init__()
        self.mdm = mdm
        self.tableplanners = []

    #
    #     * Creates an optimized left-deep query plan using the following
    #     * heuristics.
    #     * H1. Choose the smallest table (considering selection predicates)
    #     * to be first in the join order.
    #     * H2. Add the table to the join order which
    #     * results in the smallest output.
    #
    def createPlan(self, data, tx):

        #  Step 1:  Create a TablePlanner object for each mentioned table
        for tblname in data.tables():
            tp = TablePlanner(tblname, data.pred(), tx, self.mdm)
            self.tableplanners.append(tp)

        #  Step 2:  Choose the lowest-size plan to begin the join order
        currentplan = self.getLowestSelectPlan()

        #  Step 3:  Repeatedly add a plan to the join order
        while not self.tableplanners.count() == 0:
            p = self.getLowestJoinPlan(currentplan)
            if p is not None:
                currentplan = p
            else:
                currentplan = self.getLowestProductPlan(currentplan)

        #  Step 4.  Project on the field names and return
        return ProjectPlan(currentplan, data.fields())

    def getLowestSelectPlan(self):
        besttp = None
        bestplan = None
        for tp in self.tableplanners:
            plan = tp.makeSelectPlan()
            if bestplan is None or plan.recordsOutput() < bestplan.recordsOutput():
                besttp = tp
                bestplan = plan
        self.tableplanners.remove(besttp)
        return bestplan

    def getLowestJoinPlan(self, current):
        besttp = None
        bestplan = None
        for tp in self.tableplanners:
            plan = tp.makeJoinPlan(current)
            if plan is not None and (bestplan is None or plan.recordsOutput() < bestplan.recordsOutput()):
                besttp = tp
                bestplan = plan
        if bestplan is not None:
            self.tableplanners.remove(besttp)
        return bestplan

    def getLowestProductPlan(self, current):
        besttp = None
        bestplan = None
        for tp in self.tableplanners:
            plan = tp.makeProductPlan(current)
            if bestplan is None or plan.recordsOutput() < bestplan.recordsOutput():
                besttp = tp
                bestplan = plan
        self.tableplanners.remove(besttp)
        return bestplan

    def setPlanner(self, p):
        """ method setPlanner """
        #  for use in planning views, which
        #  for simplicity this code doesn't do.
