
#
#  * The index manager.
#  * The index manager has similar functionality to the table manager.
#  * @author Edward Sciore
#
from simpledb.metadata.IndexInfo import IndexInfo
from simpledb.record.Schema import Schema
from simpledb.metadata.TableMgr import TableMgr
from simpledb.record.TableScan import TableScan


class IndexMgr(object):

    #
    #     * Create the index manager.
    #     * This constructor is called during system startup.
    #     * If the database is new, then the <i>idxcat</i> table is created.
    #     * @param isnew indicates whether this is a new database
    #     * @param tx the system startup transaction
    #
    def __init__(self, isnew, tblmgr, statmgr, tx):
        if isnew:
            sch = Schema()
            sch.addStringField("indexname", TableMgr.MAX_NAME)
            sch.addStringField("tablename", TableMgr.MAX_NAME)
            sch.addStringField("fieldname", TableMgr.MAX_NAME)
            tblmgr.createTable("idxcat", sch, tx)
        self.tblmgr = tblmgr
        self.statmgr = statmgr
        self.layout = tblmgr.getLayout("idxcat", tx)

    #
    #     * Create an index of the specified type for the specified field.
    #     * A unique ID is assigned to this index, and its information
    #     * is stored in the idxcat table.
    #     * @param idxname the name of the index
    #     * @param tblname the name of the indexed table
    #     * @param fldname the name of the indexed field
    #     * @param tx the calling transaction
    #
    def createIndex(self, idxname, tblname, fldname, tx):
        ts = TableScan(tx, "idxcat", self.layout)
        ts.insert()
        ts.setString("indexname", idxname)
        ts.setString("tablename", tblname)
        ts.setString("fieldname", fldname)
        ts.close()

    #
    #     * Return a map containing the index info for all indexes
    #     * on the specified table.
    #     * @param tblname the name of the table
    #     * @param tx the calling transaction
    #     * @return a map of IndexInfo objects, keyed by their field names
    #
    def getIndexInfo(self, tblname, tx):
        result = {}
        ts = TableScan(tx, "idxcat", self.layout)
        while ts.next():
            if ts.getString("tablename") == tblname:
                idxname = ts.getString("indexname")
                fldname = ts.getString("fieldname")
                tblLayout = self.tblmgr.getLayout(tblname, tx)
                tblsi = self.statmgr.getStatInfo(tblname, tblLayout, tx)
                ii = IndexInfo(idxname, fldname, tblLayout.schema(), tx, tblsi)
                result[fldname] = ii
        ts.close()
        return result
