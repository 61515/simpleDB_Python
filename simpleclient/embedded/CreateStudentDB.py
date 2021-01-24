import traceback

from simpledb.jdbc.SQLException import SQLException
from simpledb.jdbc.embedded.EmbeddedDriver import EmbeddedDriver


class CreateStudentDB:
    @classmethod
    def main(cls, args):
        d = EmbeddedDriver()
        url = "jdbc:simpledb:studentdb"
        try:
            conn = d.connect(url, None)
            stmt = conn.createStatement()

            # Table STUDENT
            s = "create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int)"
            stmt.executeUpdate(s)
            print("Table STUDENT created.")

            s = "insert into STUDENT(SId, SName, MajorId, GradYear) values "
            studvals = ["(1, 'joe', 10, 2021)",
                        "(2, 'amy', 20, 2020)",
                        "(3, 'max', 10, 2022)",
                        "(4, 'sue', 20, 2022)",
                        "(5, 'bob', 30, 2020)",
                        "(6, 'kim', 20, 2020)",
                        "(7, 'art', 30, 2021)",
                        "(8, 'pat', 20, 2019)",
                        "(9, 'lee', 10, 2021)"]
            for i in range(len(studvals)):
                stmt.executeUpdate(s + studvals[i])
            print("STUDENT records inserted.")

            # Table DEPT
            s = "create table DEPT(DId int, DName varchar(8))"
            stmt.executeUpdate(s)
            print("Table DEPT created.")

            s = "insert into DEPT(DId, DName) values "
            deptvals = ["(10, 'compsci')",
                        "(20, 'math')",
                        "(30, 'drama')"]
            for i in range(len(deptvals)):
                stmt.executeUpdate(s + deptvals[i])
            print("DEPT records inserted.")

            # Table COURSE
            s = "create table COURSE(CId int, Title varchar(20), DeptId int)"
            stmt.executeUpdate(s)
            print("Table COURSE created.")

            s = "insert into COURSE(CId, Title, DeptId) values "
            coursevals = ["(12, 'db systems', 10)",
                          "(22, 'compilers', 10)",
                          "(32, 'calculus', 20)",
                          "(42, 'algebra', 20)",
                          "(52, 'acting', 30)",
                          "(62, 'elocution', 30)"]
            for i in range(len(coursevals)):
                stmt.executeUpdate(s + coursevals[i])
            print("COURSE records inserted.")

            # Table SECTION
            s = "create table SECTION(SectId int, CourseId int, Prof varchar(8), YearOffered int)"
            stmt.executeUpdate(s)
            print("Table SECTION created.")

            s = "insert into SECTION(SectId, CourseId, Prof, YearOffered) values "
            sectvals = ["(13, 12, 'turing', 2018)",
                        "(23, 12, 'turing', 2019)",
                        "(33, 32, 'newton', 2019)",
                        "(43, 32, 'einstein', 2017)",
                        "(53, 62, 'brando', 2018)"]
            for i in range(len(sectvals)):
                stmt.executeUpdate(s + sectvals[i])
            print("SECTION records inserted.")

            # Table ENROLL
            s = "create table ENROLL(EId int, StudentId int, SectionId int, Grade varchar(2))"
            stmt.executeUpdate(s)
            print("Table ENROLL created.")

            s = "insert into ENROLL(EId, StudentId, SectionId, Grade) values "
            enrollvals = ["(14, 1, 13, 'A')",
                          "(24, 1, 43, 'C' )",
                          "(34, 2, 43, 'B+')",
                          "(44, 4, 33, 'B' )",
                          "(54, 4, 53, 'A' )",
                          "(64, 6, 53, 'A' )"]
            for i in range(len(enrollvals)):
                stmt.executeUpdate(s + enrollvals[i])
            print("ENROLL records inserted.")
        except SQLException as e:
            traceback.print_exc()


if __name__ == '__main__':
    import sys
    CreateStudentDB.main(sys.argv)
