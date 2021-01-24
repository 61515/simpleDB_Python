#  Find the grades of student 6.
from simpledb.index.planner.IndexSelectPlan import IndexSelectPlan
from simpledb.plan.TablePlan import TablePlan
from simpledb.query.Constant import Constant
from simpledb.server.SimpleDB import SimpleDB


class IndexSelectTest(object):
    @classmethod
    def main(cls, args):
        db = SimpleDB("studentdb")
        mdm = db.mdMgr()
        tx = db.newTx()

        #  Find the index on StudentId.
        indexes = mdm.getIndexInfo("enroll", tx)
        sidIdx = indexes.get("studentid")

        #  Get the plan for the Enroll table
        enrollplan = TablePlan(tx, "enroll", mdm)

        #  Create the selection constant
        c = Constant(6)

        #  Two different ways to use the index in simpledb:
        IndexSelectTest.useIndexManually(sidIdx, enrollplan, c)
        IndexSelectTest.useIndexScan(sidIdx, enrollplan, c)

        tx.commit()

    @staticmethod
    def useIndexManually(ii, p, c):
        #  Open a scan on the table.
        s = p.open()  # must be a table scan
        idx = ii.open()

        #  Retrieve all index records having the specified dataval.
        idx.beforeFirst(c)
        while idx.next():
            #  Use the datarid to go to the corresponding Enroll record.
            datarid = idx.getDataRid()
            s.moveToRid(datarid)  # table scans can move to a specified RID.
            print(s.getString("grade"))
        idx.close()
        s.close()

    @staticmethod
    def useIndexScan(ii, p, c):
        #  Open an index select scan on the enroll table.
        idxplan = IndexSelectPlan(p, ii, c)
        s = idxplan.open()

        while s.next():
            print(s.getString("grade"))
        s.close()


if __name__ == '__main__':
    import sys
    IndexSelectTest.main(sys.argv)
