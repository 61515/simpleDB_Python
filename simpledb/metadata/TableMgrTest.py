from simpledb.buffer.BufferMgr import BufferMgr
from simpledb.log.LogMgr import LogMgr
from simpledb.metadata.TableMgr import TableMgr
from simpledb.record.Schema import Schema
from simpledb.tx.Transaction import Transaction
from simpledb.util.File import File
from simpledb.file.FileMgr import FileMgr
from simpledb.util.Types import INTEGER


class TableMgrTest(object):
    @classmethod
    def main(cls, args):
        # db = SimpleDB("tblmgrtest", 400, 8)
        fm = FileMgr(File("tblmgrtest"), 400)
        lm = LogMgr(fm, "simpledb.log")
        bm = BufferMgr(fm, lm, 8)
        tx = Transaction(fm, lm, bm)

        tm = TableMgr(True, tx)
        sch = Schema()
        sch.addIntField("A")
        sch.addStringField("B", 9)
        tm.createTable("MyTable", sch, tx)

        layout = tm.getLayout("MyTable", tx)
        size = layout.slotSize()
        sch2 = layout.schema()
        print("MyTable has slot size " + str(size))
        print("Its fields are:")
        for fldname in sch2.fields():
            if sch2.type(fldname) == INTEGER:
                _type = "int"
            else:
                strlen = sch2.length(fldname)
                _type = "varchar(" + str(strlen) + ")"
            print(fldname + ": " + _type)
        tx.commit()


if __name__ == '__main__':
    import sys
    TableMgrTest.main(sys.argv)
