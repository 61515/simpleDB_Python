from simpledb.jdbc.SQLException import SQLException
from simpledb.jdbc.embedded.EmbeddedDriver import EmbeddedDriver
import traceback


class StudentMajor(object):
    @classmethod
    def main(cls, args):
        url = "jdbc:simpledb:studentdb"
        qry = "select SName, DName " + "from DEPT, STUDENT " + "where MajorId = DId"

        d = EmbeddedDriver()
        try:
            conn = d.connect(url, None)
            stmt = conn.createStatement()

            print("Name\tMajor")
            rs = stmt.executeQuery(qry)
            while rs.next():
                sname = rs.getString("SName")
                dname = rs.getString("DName")
                print(sname + "\t" + dname)
            rs.close()
        except SQLException as e:
            traceback.print_exc()
            # print(e)


if __name__ == '__main__':
    import sys
    StudentMajor.main(sys.argv)
