from simpledb.buffer.BufferMgr import BufferMgr
from simpledb.file.FileMgr import FileMgr
from simpledb.log.LogMgr import LogMgr
from simpledb.metadata.MetadataMgr import MetadataMgr
from simpledb.plan.BasicQueryPlanner import BasicQueryPlanner
from simpledb.plan.BasicUpdatePlanner import BasicUpdatePlanner
from simpledb.plan.Planner import Planner
from simpledb.tx.Transaction import Transaction
from simpledb.util.File import File


class PlannerTest2(object):
    @classmethod
    def main(cls, args):
        # db = SimpleDB("plannertest2")
        fm = FileMgr(File("plannertest2"), 400)
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
        print("Inserting " + str(n) + " records into T1.")
        for i in range(n):
            a = i
            b = "bbb" + str(a)
            cmd = "insert into T1(A,B) values(" + str(a) + ", '" + b + "')"
            planner.executeUpdate(cmd, tx)

        cmd = "create table T2(C int, D varchar(9))"
        planner.executeUpdate(cmd, tx)
        print("Inserting " + str(n) + " records into T2.")
        for i in range(n):
            c = n - i - 1
            d = "ddd" + str(c)
            cmd = "insert into T2(C,D) values(" + str(c) + ", '" + d + "')"
            planner.executeUpdate(cmd, tx)

        qry = "select B,D from T1,T2 where A=C"
        p = planner.createQueryPlan(qry, tx)
        s = p.open()
        while s.next:
            print(s.getString("b") + " " + s.getString("d"))
        s.close()
        tx.commit()


if __name__ == '__main__':
    import sys
    PlannerTest2.main(sys.argv)
