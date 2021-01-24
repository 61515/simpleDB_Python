#
#  * The table manager.
#  * There are methods to create a table, save the metadata
#  * in the catalog, and obtain the metadata of a
#  * previously-created table.
#  * @author Edward Sciore
#
from simpledb.record.Layout import Layout
from simpledb.record.Schema import Schema
from simpledb.record.TableScan import TableScan


class TableMgr(object):
    #  The max characters a tablename or fieldname can have.
    MAX_NAME = 16

    #
    #     * Create a new catalog manager for the database system.
    #     * If the database is new, the two catalog tables
    #     * are created.
    #     * @param isNew has the value true if the database is new
    #     * @param tx the startup transaction
    #
    def __init__(self, isNew, tx):
        tcatSchema = Schema()
        tcatSchema.addStringField("tblname", TableMgr.MAX_NAME)
        tcatSchema.addIntField("slotsize")
        self.tcatLayout = Layout(tcatSchema)
        fcatSchema = Schema()
        fcatSchema.addStringField("tblname", TableMgr.MAX_NAME)
        fcatSchema.addStringField("fldname", TableMgr.MAX_NAME)
        fcatSchema.addIntField("type")
        fcatSchema.addIntField("length")
        fcatSchema.addIntField("offset")
        self.fcatLayout = Layout(fcatSchema)
        if isNew:
            self.createTable("tblcat", tcatSchema, tx)
            self.createTable("fldcat", fcatSchema, tx)

    #
    #     * Create a new table having the specified name and schema.
    #     * @param tblname the name of the new table
    #     * @param sch the table's schema
    #     * @param tx the transaction creating the table
    #
    def createTable(self, tblname, sch, tx):
        layout = Layout(sch)
        #  insert one record into tblcat
        tcat = TableScan(tx, "tblcat", self.tcatLayout)
        tcat.insert()
        tcat.setString("tblname", tblname)
        tcat.setInt("slotsize", layout.slotSize())
        tcat.close()

        #  insert a record into fldcat for each field
        fcat = TableScan(tx, "fldcat", self.fcatLayout)
        for fldname in sch.fields():
            fcat.insert()
            fcat.setString("tblname", tblname)
            fcat.setString("fldname", fldname)
            fcat.setInt("type", sch.type(fldname))
            fcat.setInt("length", sch.length(fldname))
            fcat.setInt("offset", layout.offset(fldname))
        fcat.close()

    #
    #     * Retrieve the layout of the specified table
    #     * from the catalog.
    #     * @param tblname the name of the table
    #     * @param tx the transaction
    #     * @return the table's stored metadata
    #
    def getLayout(self, tblname, tx):
        size = -1
        tcat = TableScan(tx, "tblcat", self.tcatLayout)
        while tcat.next():
            if tcat.getString("tblname") == tblname:
                size = tcat.getInt("slotsize")
                break
        tcat.close()
        sch = Schema()
        offsets = {}
        fcat = TableScan(tx, "fldcat", self.fcatLayout)
        while fcat.next():
            if fcat.getString("tblname") == tblname:
                fldname = fcat.getString("fldname")
                fldtype = fcat.getInt("type")
                fldlen = fcat.getInt("length")
                offset = fcat.getInt("offset")
                offsets[fldname] = offset
                sch.addField(fldname, fldtype, fldlen)
        fcat.close()
        return Layout(sch, offsets, size)
