from simpledb.buffer.BufferMgr import BufferMgr
from simpledb.log.LogMgr import LogMgr
from simpledb.metadata.MetadataMgr import MetadataMgr
from simpledb.record.Schema import Schema
from simpledb.record.TableScan import TableScan
from simpledb.tx.Transaction import Transaction
from simpledb.util.File import File
from simpledb.file.FileMgr import FileMgr
from simpledb.util.Types import INTEGER
import random


class MetadataMgrTest(object):
    @classmethod
    def main(cls, args):
        # db = SimpleDB("metadatamgrtest", 400, 8)
        fm = FileMgr(File("metadatamgrtest"), 400)
        lm = LogMgr(fm, "simpledb.log")
        bm = BufferMgr(fm, lm, 8)
        tx = Transaction(fm, lm, bm)

        mdm = MetadataMgr(True, tx)
        sch = Schema()
        sch.addIntField("A")
        sch.addStringField("B", 9)

        #  Part 1: Table Metadata
        mdm.createTable("MyTable", sch, tx)
        layout = mdm.getLayout("MyTable", tx)
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

        #  Part 2: Statistics Metadata
        ts = TableScan(tx, "MyTable", layout)
        for i in range(50):
            ts.insert()
            n = random.randint(0, 50)
            ts.setInt("A", n)
            ts.setString("B", "rec" + str(n))

        si = mdm.getStatInfo("MyTable", layout, tx)
        print("B(MyTable) = " + str(si.blocksAccessed()))
        print("R(MyTable) = " + str(si.recordsOutput()))
        print("V(MyTable,A) = " + str(si.distinctValues("A")))
        print("V(MyTable,B) = " + str(si.distinctValues("B")))

        #  Part 3: View Metadata
        viewdef = "select B from MyTable where A = 1"
        mdm.createView("viewA", viewdef, tx)
        v = mdm.getViewDef("viewA", tx)
        print("View def = " + v)

        #  Part 4: Index Metadata
        mdm.createIndex("indexA", "MyTable", "A", tx)
        mdm.createIndex("indexB", "MyTable", "B", tx)
        idxmap = mdm.getIndexInfo("MyTable", tx)
        ii = idxmap.get('A')
        print(ii)
        print("B(indexA) = " + str(ii.blocksAccessed()))
        print("R(indexA) = " + str(ii.recordsOutput()))
        print("V(indexA,A) = " + str(ii.distinctValues("A")))
        print("V(indexA,B) = " + str(ii.distinctValues("B")))

        ii = idxmap.get("B")
        print("B(indexB) = " + str(ii.blocksAccessed()))
        print("R(indexB) = " + str(ii.recordsOutput()))
        print("V(indexB,A) = " + str(ii.distinctValues("A")))
        print("V(indexB,B) = " + str(ii.distinctValues("B")))
        tx.commit()


if __name__ == '__main__':
    import sys
    MetadataMgrTest.main(sys.argv)
