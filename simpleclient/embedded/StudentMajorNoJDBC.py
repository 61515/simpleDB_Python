#  This is a version of the StudentMajor program that
#  * accesses the SimpleDB classes directly (instead of
#  * connecting to it as a JDBC client).
#  *
#  * These kind of programs are useful for debugging
#  * your changes to the SimpleDB source code.
#
from simpledb.server.SimpleDB import SimpleDB
import traceback


class StudentMajorNoJDBC(object):
    @classmethod
    def main(cls, args):
        try:
            #  analogous to the driver
            db = SimpleDB("studentdb")

            #  analogous to the connection
            tx = db.newTx()
            planner = db.planner()

            #  analogous to the statement
            qry = "select SName, DName " + "from DEPT, STUDENT " + "where MajorId = DId"
            p = planner.createQueryPlan(qry, tx)

            #  analogous to the result set
            s = p.open()

            print("Name\tMajor")
            while s.next():
                sname = s.getString("sname")  # SimpleDB stores field names
                dname = s.getString("dname")  # in lower case
                print(sname + "\t" + dname)
            s.close()
            tx.commit()
        except Exception as e:
            traceback.print_exc()
            # print(e)


if __name__ == '__main__':
    import sys
    StudentMajorNoJDBC.main(sys.argv)
