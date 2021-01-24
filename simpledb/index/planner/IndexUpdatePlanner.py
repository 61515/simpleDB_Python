#
#  * A modification of the basic update planner.
#  * It dispatches each update statement to the corresponding
#  * index planner.
#  * @author Edward Sciore
#
from simpledb.plan.SelectPlan import SelectPlan
from simpledb.plan.TablePlan import TablePlan
from simpledb.plan.UpdatePlanner import UpdatePlanner


class IndexUpdatePlanner(UpdatePlanner):

    def __init__(self, mdm):
        super(IndexUpdatePlanner, self).__init__()
        self.mdm = mdm

    def executeInsert(self, data, tx):
        tblname = data.tableName()
        p = TablePlan(tx, tblname, self.mdm)

        #  first, insert the record
        s = p.open()
        s.insert()
        rid = s.getRid()

        #  then modify each field, inserting an index record if appropriate
        indexes = self.mdm.getIndexInfo(tblname, tx)
        valIter = data.vals().__iter__()
        for fldname in data.fields():
            val = valIter.__next__()
            s.setVal(fldname, val)

            ii = indexes.get(fldname)
            if ii is not None:
                idx = ii.open()
                idx.insert(val, rid)
                idx.close()
        s.close()
        return 1

    def executeDelete(self, data, tx):
        tblname = data.tableName()
        p = TablePlan(tx, tblname, self.mdm)
        p = SelectPlan(p, data.pred())
        indexes = self.mdm.getIndexInfo(tblname, tx)

        s = p.open()
        count = 0
        while s.next():
            #  first, delete the record's RID from every index
            rid = s.getRid()
            for fldname in indexes.keys():
                val = s.getVal(fldname)
                idx = indexes.get(fldname).open()
                idx.delete(val, rid)
                idx.close()
            #  then delete the record
            s.delete()
            count += 1
        s.close()
        return count

    def executeModify(self, data, tx):
        tblname = data.tableName()
        fldname = data.targetField()
        p = TablePlan(tx, tblname, self.mdm)
        p = SelectPlan(p, data.pred())

        ii = self.mdm.getIndexInfo(tblname, tx).get(fldname)
        idx = None if (ii is None) else ii.open()

        s = p.open()
        count = 0
        while s.next():
            #  first, update the record
            newval = data.newValue().evaluate(s)
            oldval = s.getVal(fldname)
            s.setVal(data.targetField(), newval)

            #  then update the appropriate index, if it exists
            if idx is not None:
                rid = s.getRid()
                idx.delete(oldval, rid)
                idx.insert(newval, rid)
            count += 1
        if idx is not None:
            idx.close()
        s.close()
        return count

    def executeCreateTable(self, data, tx):
        self.mdm.createTable(data.tableName(), data.newSchema(), tx)
        return 0

    def executeCreateView(self, data, tx):
        self.mdm.createView(data.viewName(), data.viewDef(), tx)
        return 0

    def executeCreateIndex(self, data, tx):
        self.mdm.createIndex(data.indexName(), data.tableName(), data.fieldName(), tx)
        return 0
