from simpledb.plan.TablePlan import TablePlan
from simpledb.query.Constant import Constant
from simpledb.server.SimpleDB import SimpleDB


class IndexRetrievalTest(object):
    @classmethod
    def main(cls, args):
        db = SimpleDB("studentdb")
        tx = db.newTx()
        mdm = db.mdMgr()

        #  Open a scan on the data table.
        studentplan = TablePlan(tx, "student", mdm)
        studentscan = studentplan.open()

        #  Open the index on MajorId.
        indexes = mdm.getIndexInfo("student", tx)
        ii = indexes.get("majorid")
        idx = ii.open()

        #  Retrieve all index records having a dataval of 20.
        idx.beforeFirst(Constant(20))
        while idx.next():
            #  Use the datarid to go to the corresponding STUDENT record.
            datarid = idx.getDataRid()
            studentscan.moveToRid(datarid)
            print(studentscan.getString("sname"))

        #  Close the index and the data table.
        idx.close()
        studentscan.close()
        tx.commit()


if __name__ == '__main__':
    import sys
    IndexRetrievalTest.main(sys.argv)
