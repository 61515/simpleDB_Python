from simpledb.buffer.BufferMgr import BufferMgr
from simpledb.log.LogMgr import LogMgr
from simpledb.file.FileMgr import FileMgr
from simpledb.query.Expression import Expression
from simpledb.query.Predicate import Predicate
from simpledb.query.ProductScan import ProductScan
from simpledb.query.ProjectScan import ProjectScan
from simpledb.query.SelectScan import SelectScan
from simpledb.query.Term import Term
from simpledb.record.Layout import Layout
from simpledb.record.Schema import Schema
from simpledb.record.TableScan import TableScan
from simpledb.tx.Transaction import Transaction
from simpledb.util.File import File


class ScanTest2(object):
    @classmethod
    def main(cls, args):
        # db = SimpleDB("scantest2")
        fm = FileMgr(File("scantest2"), 400)
        lm = LogMgr(fm, "simpledb.log")
        bm = BufferMgr(fm, lm, 8)
        tx = Transaction(fm, lm, bm)

        sch1 = Schema()
        sch1.addIntField("A")
        sch1.addStringField("B", 9)
        layout1 = Layout(sch1)
        us1 = TableScan(tx, "T1", layout1)
        us1.beforeFirst()
        n = 200
        print("Inserting " + str(n) + " records into T1.")
        for i in range(n):
            us1.insert()
            us1.setInt("A", i)
            us1.setString("B", "bbb" + str(i))
        us1.close()

        sch2 = Schema()
        sch2.addIntField("C")
        sch2.addStringField("D", 9)
        layout2 = Layout(sch2)
        us2 = TableScan(tx, "T2", layout2)
        us2.beforeFirst()
        print("Inserting " + str(n) + " records into T2.")
        for i in range(n):
            us2.insert()
            us2.setInt("C", n - i - 1)
            us2.setString("D", "ddd" + str((n - i - 1)))
        us2.close()

        s1 = TableScan(tx, "T1", layout1)
        s2 = TableScan(tx, "T2", layout2)
        s3 = ProductScan(s1, s2)
        #  selecting all records where A=C
        t = Term(Expression("A"), Expression("C"))
        pred = Predicate(t)
        print("The predicate is " + pred.__str__())
        s4 = SelectScan(s3, pred)

        #  projecting on [B,D]
        c = ["B", "D"]
        s5 = ProjectScan(s4, c)
        while s5.next():
            print(s5.getString("B") + " " + s5.getString("D"))
        s5.close()
        tx.commit()


if __name__ == '__main__':
    import sys
    ScanTest2.main(sys.argv)
