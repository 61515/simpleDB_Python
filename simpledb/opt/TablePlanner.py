#
#  * This class contains methods for planning a single table.
#  * @author Edward Sciore
#
from simpledb.index.planner.IndexJoinPlan import IndexJoinPlan
from simpledb.index.planner.IndexSelectPlan import IndexSelectPlan
from simpledb.multibuffer.MultibufferProductPlan import MultibufferProductPlan
from simpledb.plan.SelectPlan import SelectPlan
from simpledb.plan.TablePlan import TablePlan


class TablePlanner(object):

    #
    #     * Creates a new table planner.
    #     * The specified predicate applies to the entire query.
    #     * The table planner is responsible for determining
    #     * which portion of the predicate is useful to the table,
    #     * and when indexes are useful.
    #     * @param tblname the name of the table
    #     * @param mypred the query predicate
    #     * @param tx the calling transaction
    #
    def __init__(self, tblname, mypred, tx, mdm):
        self.mypred = mypred
        self.tx = tx
        self.myplan = TablePlan(tx, tblname, mdm)
        self.myschema = self.myplan.schema()
        self.indexes = mdm.getIndexInfo(tblname, tx)

    #
    #     * Constructs a select plan for the table.
    #     * The plan will use an indexselect, if possible.
    #     * @return a select plan for the table.
    #
    def makeSelectPlan(self):
        p = self.makeIndexSelect()
        if p is None:
            p = self.myplan
        return self.addSelectPred(p)

    #
    #     * Constructs a join plan of the specified plan
    #     * and the table.  The plan will use an indexjoin, if possible.
    #     * (Which means that if an indexselect is also possible,
    #     * the indexjoin operator takes precedence.)
    #     * The method returns null if no join is possible.
    #     * @param current the specified plan
    #     * @return a join plan of the plan and this table
    #
    def makeJoinPlan(self, current):
        currsch = current.schema()
        joinpred = self.mypred.joinSubPred(self.myschema, currsch)
        if joinpred is None:
            return None
        p = self.makeIndexJoin(current, currsch)
        if p is None:
            p = self.makeProductJoin(current, currsch)
        return p

    #
    #     * Constructs a product plan of the specified plan and
    #     * this table.
    #     * @param current the specified plan
    #     * @return a product plan of the specified plan and this table
    #
    def makeProductPlan(self, current):
        p = self.addSelectPred(self.myplan)
        return MultibufferProductPlan(self.tx, current, p)

    def makeIndexSelect(self):
        for fldname in self.indexes.keys():
            val = self.mypred.equatesWithConstant(fldname)
            if val is not None:
                ii = self.indexes.get(fldname)
                print("index on " + fldname + " used")
                return IndexSelectPlan(self.myplan, ii, val)
        return None

    def makeIndexJoin(self, current, currsch):
        for fldname in self.indexes.keys():
            outerfield = self.mypred.equatesWithField(fldname)
            if outerfield is not None and currsch.hasField(outerfield):
                ii = self.indexes.get(fldname)
                p = IndexJoinPlan(current, self.myplan, ii, outerfield)
                p = self.addSelectPred(p)
                return self.addJoinPred(p, currsch)
        return None

    def makeProductJoin(self, current, currsch):
        p = self.makeProductPlan(current)
        return self.addJoinPred(p, currsch)

    def addSelectPred(self, p):
        selectpred = self.mypred.selectSubPred(self.myschema)
        if selectpred is not None:
            return SelectPlan(p, selectpred)
        else:
            return p

    def addJoinPred(self, p, currsch):
        joinpred = self.mypred.joinSubPred(currsch, self.myschema)
        if joinpred is not None:
            return SelectPlan(p, joinpred)
        else:
            return p
