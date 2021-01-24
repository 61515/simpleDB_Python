#  Find the grades of all students.
from simpledb.index.planner.IndexJoinPlan import IndexJoinPlan
from simpledb.plan.TablePlan import TablePlan
from simpledb.server.SimpleDB import SimpleDB


class IndexJoinTest(object):
    @classmethod
    def main(cls, args):
        db = SimpleDB("studentdb")
        mdm = db.mdMgr()
        tx = db.newTx()

        #  Find the index on StudentId.
        indexes = mdm.getIndexInfo("enroll", tx)
        sidIdx = indexes.get("studentid")

        #  Get plans for the Student and Enroll tables
        studentplan = TablePlan(tx, "student", mdm)
        enrollplan = TablePlan(tx, "enroll", mdm)

        #  Two different ways to use the index in simpledb:
        IndexJoinTest.useIndexManually(studentplan, enrollplan, sidIdx, "sid")
        IndexJoinTest.useIndexScan(studentplan, enrollplan, sidIdx, "sid")
        tx.commit()

    @staticmethod
    def useIndexManually(p1, p2, ii, joinfield):
        #  Open scans on the tables.
        s1 = p1.open()
        s2 = p2.open()  # must be a table scan
        idx = ii.open()

        #  Loop through s1 records. For each value of the join field,
        #  use the index to find the matching s2 records.
        while s1.next():
            c = s1.getVal(joinfield)
            idx.beforeFirst(c)
            while idx.next():
                #  Use each datarid to go to the corresponding Enroll record.
                datarid = idx.getDataRid()
                s2.moveToRid(datarid)  # table scans can move to a specified RID.
                print(s2.getString("grade"))
        idx.close()
        s1.close()
        s2.close()

    @staticmethod
    def useIndexScan(p1, p2, ii, joinfield):
        #  Open an index join scan on the table.
        idxplan = IndexJoinPlan(p1, p2, ii, joinfield)
        s = idxplan.open()

        while s.next():
            print(s.getString("grade"))
        s.close()


if __name__ == '__main__':
    import sys
    IndexJoinTest.main(sys.argv)
