from simpledb.buffer.BufferMgr import BufferMgr
from simpledb.log.LogMgr import LogMgr
from simpledb.metadata.MetadataMgr import MetadataMgr
from simpledb.plan.ProjectPlan import ProjectPlan
from simpledb.plan.SelectPlan import SelectPlan
from simpledb.plan.TablePlan import TablePlan
from simpledb.query.Constant import Constant
from simpledb.query.Expression import Expression
from simpledb.query.Predicate import Predicate
from simpledb.query.Term import Term
from simpledb.tx.Transaction import Transaction
from simpledb.util.File import File


class SingleTablePlanTest(object):
    @classmethod
    def main(cls, args):
        # db = SimpleDB("studentdb")
        fm = FileMgr(File("studentdb"), 400)
        lm = LogMgr(fm, "simpledb.log")
        bm = BufferMgr(fm, lm, 8)
        tx = Transaction(fm, lm, bm)

        isnew = fm.isNew()
        mdm = MetadataMgr(isnew, tx)

        # the STUDENT node
        p1 = TablePlan(tx, "student", mdm)

        #  the Select node for "major = 10"
        t = Term(Expression("majorid"), Expression(Constant(10)))
        pred = Predicate(t)
        p2 = SelectPlan(p1, pred)

        #  the Select node for "gradyear = 2020"
        t2 = Term(Expression("gradyear"), Expression(Constant(2020)))
        pred2 = Predicate(t2)
        p3 = SelectPlan(p2, pred2)

        #  the Project node
        c = ["sname", "majorid", "gradyear"]
        p4 = ProjectPlan(p3, c)

        #  Look at R(p) and B(p) for each plan p.
        SingleTablePlanTest.printStats(1, p1)
        SingleTablePlanTest.printStats(2, p2)
        SingleTablePlanTest.printStats(3, p3)
        SingleTablePlanTest.printStats(4, p4)

        #  Change p2 to be p2, p3, or p4 to see the other scans in action.
        #  Changing p2 to p4 will throw an exception because SID is not in the projection list.
        s = p2.open()
        while s.next:
            print(str(s.getInt("sid")) + " " + s.getString("sname")
                  + " " + str(s.getInt("majorid")) + " " + str(s.getInt("gradyear")))
        s.close()

    @staticmethod
    def printStats(n, p):
        print("Here are the stats for plan p" + str(n))
        print("\tR(p" + str(n) + "): " + str(p.recordsOutput()))
        print("\tB(p" + str(n) + "): " + str(p.blocksAccessed()))
        print()


if __name__ == '__main__':
    import sys
    SingleTablePlanTest.main(sys.argv)
