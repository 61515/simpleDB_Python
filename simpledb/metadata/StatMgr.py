#
#  * The statistics manager is responsible for
#  * keeping statistical information about each table.
#  * The manager does not store this information in the database.
#  * Instead, it calculates this information on system startup,
#  * and periodically refreshes it.
#  * @author Edward Sciore
#
from simpledb.metadata.StatInfo import StatInfo
from simpledb.record.TableScan import TableScan
from simpledb.util.Synchronized import synchronized
import threading


class StatMgr(object):

    #
    #     * Create the statistics manager.
    #     * The initial statistics are calculated by
    #     * traversing the entire database.
    #     * @param tx the startup transaction
    #
    def __init__(self, tblMgr, tx):
        self.tblMgr = tblMgr
        self.lock = threading.Lock()
        numcalls = 0
        self.tablestats = None

        self.refreshStatistics(tx)

    #
    #     * Return the statistical information about the specified table.
    #     * @param tblname the name of the table
    #     * @param layout the table's layout
    #     * @param tx the calling transaction
    #     * @return the statistical information about the table
    #
    @synchronized
    def getStatInfo(self, tblname, layout, tx):
        self.numcalls += 1
        if self.numcalls > 100:
            self.lock.release()
            self.refreshStatistics(tx)
            self.lock.acquire()
        si = self.tablestats.get(tblname)
        if si is None:
            self.lock.release()
            si = self.calcTableStats(tblname, layout, tx)
            self.lock.acquire()
            self.tablestats[tblname] = si
        return si

    @synchronized
    def refreshStatistics(self, tx):
        self.tablestats = {}
        self.numcalls = 0
        tcatlayout = self.tblMgr.getLayout("tblcat", tx)
        tcat = TableScan(tx, "tblcat", tcatlayout)
        while tcat.next():
            tblname = tcat.getString("tblname")
            layout = self.tblMgr.getLayout(tblname, tx)
            self.lock.release()
            si = self.calcTableStats(tblname, layout, tx)
            self.lock.acquire()
            self.tablestats[tblname] = si
        tcat.close()

    @synchronized
    def calcTableStats(self, tblname, layout, tx):
        numRecs = 0
        numblocks = 0
        ts = TableScan(tx, tblname, layout)
        while ts.next():
            numRecs += 1
            numblocks = ts.getRid().blockNumber() + 1
        ts.close()
        return StatInfo(numblocks, numRecs)
