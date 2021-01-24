#
#  * The Plan class for the <i>sort</i> operator.
#  * @author Edward Sciore
#
from simpledb.materialize.MaterializePlan import MaterializePlan
from simpledb.materialize.RecordComparator import RecordComparator
from simpledb.materialize.SortScan import SortScan
from simpledb.materialize.TempTable import TempTable
from simpledb.plan.Plan import Plan


class SortPlan(Plan):

    #
    #     * Create a sort plan for the specified query.
    #     * @param p the plan for the underlying query
    #     * @param sortfields the fields to sort by
    #     * @param tx the calling transaction
    #
    def __init__(self, tx, p, sortfields):
        super(SortPlan, self).__init__()
        self.tx = tx
        self.p = p
        self.sch = p.schema()
        self.comp = RecordComparator(sortfields)

    #
    #     * This method is where most of the action is.
    #     * Up to 2 sorted temporary tables are created,
    #     * and are passed into SortScan for final merging.
    #     * @see Plan#open()
    #
    def open(self):
        src = self.p.open()
        runs = self.splitIntoRuns(src)
        src.close()
        while len(runs) > 2:
            runs = self.doAMergeIteration(runs)
        return SortScan(runs, self.comp)

    #
    #     * Return the number of blocks in the sorted table,
    #     * which is the same as it would be in a
    #     * materialized table.
    #     * It does <i>not</i> include the one-time cost
    #     * of materializing and sorting the records.
    #     * @see Plan#blocksAccessed()
    #
    def blocksAccessed(self):
        #  does not include the one-time cost of sorting
        mp = MaterializePlan(self.tx, self.p)  # not opened; just for analysis
        return mp.blocksAccessed()

    #
    #     * Return the number of records in the sorted table,
    #     * which is the same as in the underlying query.
    #     * @see Plan#recordsOutput()
    #
    def recordsOutput(self):
        return self.p.recordsOutput()

    #
    #     * Return the number of distinct field values in
    #     * the sorted table, which is the same as in
    #     * the underlying query.
    #     * @see Plan#distinctValues(String)
    #
    def distinctValues(self, fldname):
        return self.p.distinctValues(fldname)

    #
    #     * Return the schema of the sorted table, which
    #     * is the same as in the underlying query.
    #     * @see Plan#schema()
    #
    def schema(self):
        return self.sch

    def splitIntoRuns(self, src):
        temps = []
        src.beforeFirst()
        if not src.next():
            return temps
        currenttemp = TempTable(self.tx, self.sch)
        temps.append(currenttemp)
        currentscan = currenttemp.open()
        while self.copy(src, currentscan):
            if self.comp.compare(src, currentscan) < 0:
                #  start a new run
                currentscan.close()
                currenttemp = TempTable(self.tx, self.sch)
                temps.append(currenttemp)
                currentscan = currenttemp.open()
        currentscan.close()
        return temps

    def doAMergeIteration(self, runs):
        result = []
        while len(runs) > 1:
            p1 = runs.pop(0)
            p2 = runs.pop(0)
            result.append(self.mergeTwoRuns(p1, p2))
        if len(runs) == 1:
            result.append(runs.get(0))
        return result

    def mergeTwoRuns(self, p1, p2):
        src1 = p1.open()
        src2 = p2.open()
        result = TempTable(self.tx, self.sch)
        dest = result.open()

        hasmore1 = src1.next()
        hasmore2 = src2.next()
        while hasmore1 and hasmore2:
            if self.comp.compare(src1, src2) < 0:
                hasmore1 = self.copy(src1, dest)
            else:
                hasmore2 = self.copy(src2, dest)

        if hasmore1:
            while hasmore1:
                hasmore1 = self.copy(src1, dest)
        else:
            while hasmore2:
                hasmore2 = self.copy(src2, dest)
        src1.close()
        src2.close()
        dest.close()
        return result

    def copy(self, src, dest):
        dest.insert()
        for fldname in self.sch.fields():
            dest.setVal(fldname, src.getVal(fldname))
        return src.next()
