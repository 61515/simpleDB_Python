from simpledb.metadata.TableMgr import TableMgr
from simpledb.record.Schema import Schema
from simpledb.record.TableScan import TableScan


class ViewMgr(object):
    #  the max chars in a view definition.
    MAX_VIEWDEF = 100

    def __init__(self, isNew, tblMgr, tx):
        self.tblMgr = tblMgr
        if isNew:
            sch = Schema()
            sch.addStringField("viewname", TableMgr.MAX_NAME)
            sch.addStringField("viewdef", ViewMgr.MAX_VIEWDEF)
            tblMgr.createTable("viewcat", sch, tx)

    def createView(self, vname, vdef, tx):
        layout = self.tblMgr.getLayout("viewcat", tx)
        ts = TableScan(tx, "viewcat", layout)
        ts.insert()
        ts.setString("viewname", vname)
        ts.setString("viewdef", vdef)
        ts.close()

    def getViewDef(self, vname, tx):
        result = None
        layout = self.tblMgr.getLayout("viewcat", tx)
        ts = TableScan(tx, "viewcat", layout)
        while ts.next():
            if ts.getString("viewname") == vname:
                result = ts.getString("viewdef")
                break
        ts.close()
        return result
