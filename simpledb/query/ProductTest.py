from simpledb.buffer.BufferMgr import BufferMgr
from simpledb.log.LogMgr import LogMgr
from simpledb.query.ProductScan import ProductScan
from simpledb.record.Layout import Layout
from simpledb.record.Schema import Schema
from simpledb.record.TableScan import TableScan
from simpledb.tx.Transaction import Transaction
from simpledb.util.File import File
from simpledb.file.FileMgr import FileMgr


class ProductTest(object):
    @classmethod
    def main(cls, args):
        # db = SimpleDB("producttest")
        fm = FileMgr(File("producttest"), 400)
        lm = LogMgr(fm, "simpledb.log")
        bm = BufferMgr(fm, lm, 8)
        tx = Transaction(fm, lm, bm)

        sch1 = Schema()
        sch1.addIntField("A")
        sch1.addStringField("B", 9)
        layout1 = Layout(sch1)
        ts1 = TableScan(tx, "T1", layout1)

        sch2 = Schema()
        sch2.addIntField("C")
        sch2.addStringField("D", 9)
        layout2 = Layout(sch2)
        ts2 = TableScan(tx, "T2", layout2)

        ts1.beforeFirst()
        n = 200
        print("Inserting " + str(n) + " records into T1.")
        for i in range(n):
            ts1.insert()
            ts1.setInt("A", i)
            ts1.setString("B", "aaa" + str(i))
        ts1.close()

        ts2.beforeFirst()
        print("Inserting " + str(n) + " records into T2.")
        for i in range(n):
            ts2.insert()
            ts2.setInt("C", n - i - 1)
            ts2.setString("D", "bbb" + str((n - i - 1)))
        ts2.close()

        s1 = TableScan(tx, "T1", layout1)
        s2 = TableScan(tx, "T2", layout2)
        s3 = ProductScan(s1, s2)
        while s3.next():
            print(s3.getString("B"))
        s3.close()
        tx.commit()


if __name__ == '__main__':
    import sys
    ProductTest.main(sys.argv)
