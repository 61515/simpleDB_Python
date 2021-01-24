from simpledb.plan.TablePlan import TablePlan
from simpledb.server.SimpleDB import SimpleDB


class IndexUpdateTest(object):
    @classmethod
    def main(cls, args):
        db = SimpleDB("studentdb")
        tx = db.newTx()
        mdm = db.mdMgr()
        studentplan = TablePlan(tx, "student", mdm)
        studentscan = studentplan.open()

        #  Create a map containing all indexes for STUDENT.
        indexes = {}
        idxinfo = mdm.getIndexInfo("student", tx)
        for fldname in idxinfo.keySet():
            idx = idxinfo.get(fldname).open()
            indexes[fldname] = idx

        #  Task 1: insert a new STUDENT record for Sam
        #     First, insert the record into STUDENT.
        studentscan.insert()
        studentscan.setInt("sid", 11)
        studentscan.setString("sname", "sam")
        studentscan.setInt("gradyear", 2023)
        studentscan.setInt("majorid", 30)

        #     Then insert a record into each of the indexes.
        datarid = studentscan.getRid()
        for fldname in indexes.keys():
            dataval = studentscan.getVal(fldname)
            idx = indexes.get(fldname)
            idx.insert(dataval, datarid)

        #  Task 2: find and delete Joe's record
        studentscan.beforeFirst()
        while studentscan.next():
            if studentscan.getString("sname") == "joe":

                #  First, delete the index records for Joe.
                joeRid = studentscan.getRid()
                for fldname in indexes.keys():
                    dataval = studentscan.getVal(fldname)
                    idx = indexes.get(fldname)
                    idx.delete(dataval, joeRid)

                #  Then delete Joe's record in STUDENT.
                studentscan.delete()
                break

        #  Print the records to verify the updates.
        studentscan.beforeFirst()
        while studentscan.next():
            print(studentscan.getString("sname") + " " + str(studentscan.getInt("sid")))
        studentscan.close()

        for idx in indexes.values():
            idx.close()
        tx.commit()


if __name__ == '__main__':
    import sys
    IndexUpdateTest.main(sys.argv)
