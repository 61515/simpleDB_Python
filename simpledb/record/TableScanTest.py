from simpledb.buffer.BufferMgr import BufferMgr
from simpledb.log.LogMgr import LogMgr
from simpledb.record.Layout import Layout
from simpledb.record.Schema import Schema
from simpledb.record.TableScan import TableScan
from simpledb.tx.Transaction import Transaction
from simpledb.util.File import File
from simpledb.file.FileMgr import FileMgr
import random


class TableScanTest(object):
    @classmethod
    def main(cls, args):
        # db = SimpleDB("tabletest", 400, 8)
        fm = FileMgr(File("tabletest"), 400)
        lm = LogMgr(fm, "simpledb.log")
        bm = BufferMgr(fm, lm, 8)
        tx = Transaction(fm, lm, bm)

        sch = Schema()
        sch.addIntField("A")
        sch.addStringField("B", 9)
        layout = Layout(sch)
        for fldname in layout.schema().fields():
            offset = layout.offset(fldname)
            print(fldname + " has offset " + str(offset))

        print("Filling the table with 50 random records.")
        ts = TableScan(tx, "T", layout)
        for i in range(50):
            ts.insert()
            n = random.randint(0, 50)
            ts.setInt("A", n)
            ts.setString("B", "rec" + str(n))
            print("inserting into slot " + ts.getRid().__str__() + ": {" + str(n) + ", " + "rec" + str(n) + "}")

        print("Deleting these records, whose A-values are less than 25.")
        count = 0
        ts.beforeFirst()
        while ts.next():
            a = ts.getInt("A")
            b = ts.getString("B")
            if a < 25:
                count += 1
                print("slot " + ts.getRid().__str__() + ": {" + str(a) + ", " + b + "}")
                ts.delete()
        print(str(count) + " values under 10 were deleted.\n")

        print("Here are the remaining records.")
        ts.beforeFirst()
        while ts.next():
            a = ts.getInt("A")
            b = ts.getString("B")
            print("slot " + ts.getRid().__str__() + ": {" + str(a) + ", " + b + "}")
        ts.close()
        tx.commit()


if __name__ == '__main__':
    import sys
    TableScanTest.main(sys.argv)
