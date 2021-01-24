from simpledb.buffer.BufferMgr import BufferMgr
from simpledb.file.FileMgr import FileMgr
from simpledb.log.LogMgr import LogMgr
from simpledb.query.Constant import Constant
from simpledb.query.Expression import Expression
from simpledb.query.Predicate import Predicate
from simpledb.query.ProjectScan import ProjectScan
from simpledb.query.SelectScan import SelectScan
from simpledb.query.Term import Term
from simpledb.record.Layout import Layout
from simpledb.record.Schema import Schema
from simpledb.record.TableScan import TableScan
from simpledb.tx.Transaction import Transaction
from simpledb.util.File import File
import random


class ScanTest1(object):
    @classmethod
    def main(cls, args):
        # db = SimpleDB("scantest1")
        fm = FileMgr(File("scantest1"), 400)
        lm = LogMgr(fm, "simpledb.log")
        bm = BufferMgr(fm, lm, 8)
        tx = Transaction(fm, lm, bm)

        sch1 = Schema()
        sch1.addIntField("A")
        sch1.addStringField("B", 9)
        layout = Layout(sch1)
        s1 = TableScan(tx, "T", layout)

        s1.beforeFirst()
        n = 200
        print("Inserting " + str(n) + " random records.")
        for i in range(n):
            s1.insert()
            k = random.randint(0, 50)
            s1.setInt("A", k)
            s1.setString("B", "rec" + str(k))
        s1.close()

        s2 = TableScan(tx, "T", layout)

        #  selecting all records where A=10
        c = Constant(10)
        t = Term(Expression("A"), Expression(c))
        pred = Predicate(t)
        print("The predicate is " + pred.__str__())
        s3 = SelectScan(s2, pred)

        fields = ["B"]
        s4 = ProjectScan(s3, fields)
        while s4.next():
            print(s4.getString("B"))
        s4.close()
        tx.commit()


if __name__ == '__main__':
    import sys
    ScanTest1.main(sys.argv)
