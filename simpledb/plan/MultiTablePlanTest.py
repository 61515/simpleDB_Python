from simpledb.buffer.BufferMgr import BufferMgr
from simpledb.log.LogMgr import LogMgr
from simpledb.metadata.MetadataMgr import MetadataMgr
from simpledb.plan.ProductPlan import ProductPlan
from simpledb.plan.SelectPlan import SelectPlan
from simpledb.plan.TablePlan import TablePlan
from simpledb.query.Expression import Expression
from simpledb.query.Predicate import Predicate
from simpledb.query.Term import Term
from simpledb.tx.Transaction import Transaction
from simpledb.util.File import File


class MultiTablePlanTest(object):
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

        # the DEPT node
        p2 = TablePlan(tx, "dept", mdm)

        # the Product node for student x dept
        p3 = ProductPlan(p1, p2)

        #  the Select node for "majorid = did"
        t = Term(Expression("majorid"), Expression("did"))
        pred = Predicate(t)
        p4 = SelectPlan(p3, pred)

        #  Look at R(p) and B(p) for each plan p.
        MultiTablePlanTest.printStats(1, p1)
        MultiTablePlanTest.printStats(2, p2)
        MultiTablePlanTest.printStats(3, p3)
        MultiTablePlanTest.printStats(4, p4)

        #  Change p3 to be p4 to see the select scan in action.
        s = p3.open()
        while s.next:
            print(s.getString("sname") + " " + s.getString("dname"))
        s.close()

    @staticmethod
    def printStats(n, p):
        print("Here are the stats for plan p" + str(n))
        print("\tR(p" + str(n) + "): " + str(p.recordsOutput()))
        print("\tB(p" + str(n) + "): " + str(p.blocksAccessed()))
        print()


if __name__ == '__main__':
    import sys
    MultiTablePlanTest.main(sys.argv)
