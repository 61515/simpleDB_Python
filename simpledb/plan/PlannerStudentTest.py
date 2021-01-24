from simpledb.buffer.BufferMgr import BufferMgr
from simpledb.file.FileMgr import FileMgr
from simpledb.log.LogMgr import LogMgr
from simpledb.metadata.MetadataMgr import MetadataMgr
from simpledb.plan.BasicQueryPlanner import BasicQueryPlanner
from simpledb.plan.BasicUpdatePlanner import BasicUpdatePlanner
from simpledb.plan.Planner import Planner
from simpledb.tx.Transaction import Transaction
from simpledb.util.File import File


class PlannerStudentTest(object):
    @classmethod
    def main(cls, args):
        # db = SimpleDB("studentdb")
        fm = FileMgr(File("plannertest2"), 400)
        lm = LogMgr(fm, "simpledb.log")
        bm = BufferMgr(fm, lm, 8)
        tx = Transaction(fm, lm, bm)

        isnew = fm.isNew()
        mdm = MetadataMgr(isnew, tx)
        qp = BasicQueryPlanner(mdm)
        up = BasicUpdatePlanner(mdm)
        planner = Planner(qp, up)

        #  part 1: Process a query
        qry = "select sname, gradyear from student"
        p = planner.createQueryPlan(qry, tx)
        s = p.open()
        while s.next:
            print(s.getString("sname") + " " + str(s.getInt("gradyear")))
        s.close()

        #  part 2: Process an update command
        cmd = "delete from STUDENT where MajorId = 30"
        num = planner.executeUpdate(cmd, tx)
        print(str(num) + " students were deleted")
        tx.commit()


if __name__ == '__main__':
    import sys
    PlannerStudentTest.main(sys.argv)
