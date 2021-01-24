#
#  * The simplest, most naive query planner possible.
#  * @author Edward Sciore
#
from simpledb.parse.Parser import Parser
from simpledb.plan.ProductPlan import ProductPlan
from simpledb.plan.ProjectPlan import ProjectPlan
from simpledb.plan.QueryPlanner import QueryPlanner
from simpledb.plan.SelectPlan import SelectPlan
from simpledb.plan.TablePlan import TablePlan


class BasicQueryPlanner(QueryPlanner):

    def __init__(self, mdm):
        super(BasicQueryPlanner, self).__init__()
        self.mdm = mdm

    #
    #     * Creates a query plan as follows.  It first takes
    #     * the product of all tables and views; it then selects on the predicate;
    #     * and finally it projects on the field list.
    #
    def createPlan(self, data, tx):
        # Step 1: Create a plan for each mentioned table or view.
        plans = []
        for tblname in data.tables():
            viewdef = self.mdm.getViewDef(tblname, tx)
            if viewdef is not None:  # Recursively plan the view.
                parser = Parser(viewdef)
                viewdata = parser.query()
                plans.append(self.createPlan(viewdata, tx))
            else:
                plans.append(TablePlan(tx, tblname, self.mdm))

        # Step 2: Create the product of all table plans
        p = plans.pop(0)
        for nextplan in plans:
            p = ProductPlan(p, nextplan)

        # Step 3: Add a selection plan for the predicate
        p = SelectPlan(p, data.pred())

        # Step 4: Project on the field names
        p = ProjectPlan(p, data.fields())
        return p
