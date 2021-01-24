from simpledb.buffer.BufferMgr import BufferMgr
from simpledb.log.LogMgr import LogMgr
from simpledb.record.Layout import Layout
from simpledb.record.RecordPage import RecordPage
from simpledb.record.Schema import Schema
from simpledb.tx.Transaction import Transaction
from simpledb.util.File import File
from simpledb.file.FileMgr import FileMgr
import random


class RecordTest(object):
    @classmethod
    def main(cls, args):
        # db = SimpleDB("recordtest", 400, 8)
        fm = FileMgr(File("recordtest"), 400)
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
        blk = tx.append("testfile")
        tx.pin(blk)
        rp = RecordPage(tx, blk, layout)
        rp.format()

        print("Filling the page with random records.")
        slot = rp.insertAfter(-1)
        while slot >= 0:
            n = random.randint(0, 50)
            rp.setInt(slot, "A", n)
            rp.setString(slot, "B", "rec" + str(n))
            print("inserting into slot " + str(slot) + ": {" + str(n) + ", " + "rec" + str(n) + "}")
            slot = rp.insertAfter(slot)

        print("Deleting these records, whose A-values are less than 25.")

        count = 0
        slot = rp.nextAfter(-1)
        while slot >= 0:
            a = rp.getInt(slot, "A")
            b = rp.getString(slot, "B")
            if a < 25:
                count += 1
                print("slot " + str(slot) + ": {" + str(a) + ", " + str(b) + "}")
                rp.delete(slot)
            slot = rp.nextAfter(slot)

        print(str(count) + " values under 25 were deleted.\n")
        print("Here are the remaining records.")
        slot = rp.nextAfter(-1)

        while slot >= 0:
            a = rp.getInt(slot, "A")
            b = rp.getString(slot, "B")
            print("slot " + str(slot) + ": {" + str(a) + ", " + str(b) + "}")
            slot = rp.nextAfter(slot)
        tx.unpin(blk)
        tx.commit()


if __name__ == '__main__':
    import sys
    RecordTest.main(sys.argv)
