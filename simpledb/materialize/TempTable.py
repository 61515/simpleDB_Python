#
#  * A class that creates temporary tables.
#  * A temporary table is not registered in the catalog.
#  * The class therefore has a method getTableInfo to return the
#  * table's metadata.
#  * @author Edward Sciore
#
from simpledb.record.Layout import Layout
from simpledb.record.TableScan import TableScan
from simpledb.util.Synchronized import synchronized
import threading


class TempTable(object):
    nextTableNum = 0
    lock = threading.Lock()

    #
    #     * Allocate a name for for a new temporary table
    #     * having the specified schema.
    #     * @param sch the new table's schema
    #     * @param tx the calling transaction
    #
    def __init__(self, tx, sch):
        self.tx = tx
        self.tblname = self.nextTableName()
        self.layout = Layout(sch)

    #
    #     * Open a table scan for the temporary table.
    #
    def open(self):
        return TableScan(self.tx, self.tblname, self.layout)

    def tableName(self):
        return self.tblname

    #
    #     * Return the table's metadata.
    #     * @return the table's metadata
    #
    def getLayout(self):
        return self.layout

    @staticmethod
    @synchronized
    def nextTableName():
        TempTable.nextTableNum += 1
        return "temp" + str(TempTable.nextTableNum)
