from simpledb.buffer.BufferMgr import BufferMgr
from simpledb.log.LogMgr import LogMgr
from simpledb.metadata.MetadataMgr import MetadataMgr
from simpledb.plan.BasicQueryPlanner import BasicQueryPlanner
from simpledb.plan.BasicUpdatePlanner import BasicUpdatePlanner
from simpledb.plan.Planner import Planner
from simpledb.tx.Transaction import Transaction
from simpledb.util.File import File
from simpledb.file.FileMgr import FileMgr
import random


class PlannerTest1(object):
    @classmethod
    def main(cls, args):
        # db = SimpleDB("plannertest1")
        fm = FileMgr(File("plannertest1"), 400)
        lm = LogMgr(fm, "simpledb.log")
        bm = BufferMgr(fm, lm, 8)
        tx = Transaction(fm, lm, bm)

        isnew = fm.isNew()
        mdm = MetadataMgr(isnew, tx)
        qp = BasicQueryPlanner(mdm)
        up = BasicUpdatePlanner(mdm)
        planner = Planner(qp, up)

        cmd = "create table T1(A int, B varchar(9))"
        planner.executeUpdate(cmd, tx)
        n = 200
        print("Inserting " + str(n) + " random records.")
        for i in range(n):
            a = random.randint(0, 50)
            b = "rec" + str(a)
            cmd = "insert into T1(A,B) values(" + str(a) + ", '" + b + "')"
            planner.executeUpdate(cmd, tx)

        qry = "select B from T1 where A=10"
        p = planner.createQueryPlan(qry, tx)
        s = p.open()
        while s.next:
            print(s.getString("b"))
        s.close()
        tx.commit()


if __name__ == '__main__':
    import sys
    PlannerTest1.main(sys.argv)
