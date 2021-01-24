from simpledb.jdbc.SQLException import SQLException
from simpledb.jdbc.embedded.EmbeddedDriver import EmbeddedDriver
from simpledb.util import Types
import traceback


class SimpleIJ(object):
    # create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int)
    # insert into STUDENT(SId, SName, MajorId, GradYear) values(1, 'joe', 10, 2021)
    # select sname from student
    @classmethod
    def main(cls, args):
        print("Connect> ")
        s = sys.stdin.readline().strip('\n')
        d = EmbeddedDriver()

        try:
            conn = d.connect(s, None)
            stmt = conn.createStatement()
            print("\nSQL> ", end='')
            while True:
                cmd = sys.stdin.readline()
                if not cmd:
                    break

                # print(cmd)
                #  process one line of input
                if cmd.startswith("exit"):
                    break
                elif cmd.startswith("select"):
                    cls.doQuery(stmt, cmd)
                else:
                    cls.doUpdate(stmt, cmd)
                print("\nSQL> ", end='')
        except SQLException as e:
            print(e)

    @staticmethod
    def doQuery(stmt, cmd):
        try:
            rs = stmt.executeQuery(cmd)
            md = rs.getMetaData()
            numcols = md.getColumnCount()
            totalwidth = 0

            #  print header
            for i in range(1, numcols + 1):
                fldname = md.getColumnName(i)
                width = md.getColumnDisplaySize(i)
                totalwidth += width
                fmt = "%" + str(width) + "s"
                print(fmt % fldname)
            print()
            for i in range(totalwidth):
                print("-", end='')
            print()

            #  print records
            while rs.next():
                for i in range(1, numcols + 1):
                    fldname = md.getColumnName(i)
                    fldtype = md.getColumnType(i)
                    fmt = "%" + str(md.getColumnDisplaySize(i))
                    if fldtype == Types.INTEGER:
                        ival = rs.getInt(fldname)
                        fmt += "d"
                        print(fmt % ival)
                    else:
                        sval = rs.getString(fldname)
                        fmt += "s"
                        print(fmt % sval)
                print()
        except SQLException as e:
            print("SQL Exception: " + e.__str__())

    @staticmethod
    def doUpdate(stmt, cmd):
        try:
            howmany = stmt.executeUpdate(cmd)
            print(str(howmany) + " records processed")
        except SQLException as e:
            traceback.print_exc()
            print("SQL Exception: " + e.__str__())


if __name__ == '__main__':
    import sys
    SimpleIJ.main(sys.argv)
