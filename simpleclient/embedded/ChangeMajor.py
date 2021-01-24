from simpledb.jdbc.SQLException import SQLException
from simpledb.jdbc.embedded.EmbeddedDriver import EmbeddedDriver
import traceback


class ChangeMajor(object):
    @classmethod
    def main(cls, args):
        d = EmbeddedDriver()
        url = "jdbc:simpledb:studentdb"
        try:
            conn = d.connect(url, None)
            stmt = conn.createStatement()
            cmd = "update STUDENT " + "set MajorId=30 " + "where SName = 'amy'"
            stmt.executeUpdate(cmd)
            print("Amy is now a drama major.")
        except SQLException as e:
            traceback.print_exc()
            # print(e)


if __name__ == '__main__':
    import sys
    ChangeMajor.main(sys.argv)
