from simpledb.file.BlockId import BlockId
from simpledb.file.Page import Page
from simpledb.jdbc.SQLException import SQLException
from simpledb.jdbc.embedded.EmbeddedDriver import EmbeddedDriver
from simpledb.plan.TablePlan import TablePlan
from simpledb.server.SimpleDB import SimpleDB
from simpledb.tx.recovery.LogRecord import LogRecord
import traceback


def test1():
    d = EmbeddedDriver()
    url = "jdbc:simpledb:studentdb"
    try:
        conn = d.connect(url, None)
        stmt = conn.createStatement()

        s = "insert into STUDENT(SId, SName, MajorId, GradYear) values "
        studvals = ["(1, 'csb', 10, 2021)",
                    "(1, 'csd', 10, 2021)"]
        stmt.executeUpdate(s + studvals[0])
        stmt.executeUpdate(s + studvals[1])
        print("STUDENT records inserted.")
    except SQLException as e:
        traceback.print_exc()


def test2():
    db = SimpleDB("studentdb")
    mdm = db.mdMgr()
    tx = db.newTx()

    p1 = TablePlan(tx, "student", mdm)
    s = p1.open()
    while s.next():
        print(s.getString("sname"), s.getInt("gradyear"))


def test3():
    db = SimpleDB("studentdb")
    lm = db.logMgr()
    fm = db.fileMgr()

    filename = "simpledb.log"
    lastblock = fm.length(filename) - 1
    blk = BlockId(filename, lastblock)
    p = Page(fm.blockSize())
    fm.read(blk, p)
    iterator = lm.iterator()
    while iterator.hasNext():
        byte_array = iterator.next()
        rec = LogRecord.createLogRecord(byte_array)
        print(rec)


if __name__ == '__main__':
    test1()
    # test2()
    # test3()
