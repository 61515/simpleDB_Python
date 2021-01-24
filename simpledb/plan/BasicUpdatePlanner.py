#
#  * The basic planner for SQL update statements.
#  * @author sciore
#
from simpledb.plan.SelectPlan import SelectPlan
from simpledb.plan.TablePlan import TablePlan
from simpledb.plan.UpdatePlanner import UpdatePlanner
from simpledb.query.UpdateScan import UpdateScan


class BasicUpdatePlanner(UpdatePlanner):

    def __init__(self, mdm):
        super(BasicUpdatePlanner, self).__init__()
        self.mdm = mdm

    def executeDelete(self, data, tx):
        p = TablePlan(tx, data.tableName(), self.mdm)
        p = SelectPlan(p, data.pred())
        us = p.open()
        count = 0
        while us.next():
            us.delete()
            count += 1
        us.close()
        return count

    def executeModify(self, data, tx):
        p = TablePlan(tx, data.tableName(), self.mdm)
        p = SelectPlan(p, data.pred())
        us = p.open()
        count = 0
        while us.next():
            val = data.newValue().evaluate(us)
            us.setVal(data.targetField(), val)
            count += 1
        us.close()
        return count

    def executeInsert(self, data, tx):
        p = TablePlan(tx, data.tableName(), self.mdm)
        us = p.open()
        us.insert()
        iterator = data.vals().__iter__()
        for fldname in data.fields():
            val = iterator.__next__()
            us.setVal(fldname, val)
        us.close()
        return 1

    def executeCreateTable(self, data, tx):
        self.mdm.createTable(data.tableName(), data.newSchema(), tx)
        return 0

    def executeCreateView(self, data, tx):
        self.mdm.createView(data.viewName(), data.viewDef(), tx)
        return 0

    def executeCreateIndex(self, data, tx):
        self.mdm.createIndex(data.indexName(), data.tableName(), data.fieldName(), tx)
        return 0
