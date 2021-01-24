from simpledb.jdbc.embedded.EmbeddedDriver import EmbeddedDriver
import traceback


class FindMajors(object):
    @classmethod
    def main(cls, args):
        print("Enter a department name: ")
        major = sys.stdin.readline().strip('\n')
        url = "jdbc:simpledb:studentdb"
        qry = "select sname, gradyear " + "from student, dept " + \
              "where did = majorid " + "and dname = '" + major + "'"
        d = EmbeddedDriver()
        try:
            conn = d.connect(url, None)
            stmt = conn.createStatement()
            rs = stmt.executeQuery(qry)
            print("Here are the " + major.__str__() + " majors")
            print("Name\tGradYear")
            while rs.next():
                sname = rs.getString("sname")
                gradyear = rs.getInt("gradyear")
                print(sname + "\t" + str(gradyear))
        except Exception as e:
            traceback.print_exc()
            # print(e)


if __name__ == '__main__':
    import sys
    FindMajors.main(sys.argv)
