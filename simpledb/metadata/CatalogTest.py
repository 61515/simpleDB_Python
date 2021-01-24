from simpledb.buffer.BufferMgr import BufferMgr
from simpledb.file.FileMgr import FileMgr
from simpledb.log.LogMgr import LogMgr
from simpledb.metadata.TableMgr import TableMgr
from simpledb.record.TableScan import TableScan
from simpledb.tx.Transaction import Transaction
from simpledb.util.File import File


class CatalogTest(object):
    @classmethod
    def main(cls, args):
        # db = SimpleDB("tabletest", 400, 8)
        fm = FileMgr(File("tabletest"), 400)
        lm = LogMgr(fm, "simpledb.log")
        bm = BufferMgr(fm, lm, 8)
        tx = Transaction(fm, lm, bm)

        tm = TableMgr(True, tx)
        tcatLayout = tm.getLayout("tblcat", tx)
        print("Here are all the tables and their lengths.")
        ts = TableScan(tx, "tblcat", tcatLayout)
        while ts.next():
            tname = ts.getString("tblname")
            slotsize = ts.getInt("slotsize")
            print(tname + " " + str(slotsize))
        ts.close()

        print("\nHere are the fields for each table and their offsets")
        fcatLayout = tm.getLayout("fldcat", tx)
        ts = TableScan(tx, "fldcat", fcatLayout)
        while ts.next():
            tname = ts.getString("tblname")
            fname = ts.getString("fldname")
            offset = ts.getInt("offset")
            print(tname + " " + fname + " " + str(offset))
        ts.close()


if __name__ == '__main__':
    import sys
    CatalogTest.main(sys.argv)
