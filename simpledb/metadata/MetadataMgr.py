from simpledb.metadata.IndexMgr import IndexMgr
from simpledb.metadata.StatMgr import StatMgr
from simpledb.metadata.TableMgr import TableMgr
from simpledb.metadata.ViewMgr import ViewMgr


class MetadataMgr(object):

    def __init__(self, isnew, tx):
        self.tblmgr = TableMgr(isnew, tx)
        self.viewmgr = ViewMgr(isnew, self.tblmgr, tx)
        self.statmgr = StatMgr(self.tblmgr, tx)
        self.idxmgr = IndexMgr(isnew, self.tblmgr, self.statmgr, tx)

    def createTable(self, tblname, sch, tx):
        self.tblmgr.createTable(tblname, sch, tx)

    def getLayout(self, tblname, tx):
        return self.tblmgr.getLayout(tblname, tx)

    def createView(self, viewname, viewdef, tx):
        self.viewmgr.createView(viewname, viewdef, tx)

    def getViewDef(self, viewname, tx):
        return self.viewmgr.getViewDef(viewname, tx)

    def createIndex(self, idxname, tblname, fldname, tx):
        self.idxmgr.createIndex(idxname, tblname, fldname, tx)

    def getIndexInfo(self, tblname, tx):
        return self.idxmgr.getIndexInfo(tblname, tx)

    def getStatInfo(self, tblname, layout, tx):
        return self.statmgr.getStatInfo(tblname, layout, tx)
