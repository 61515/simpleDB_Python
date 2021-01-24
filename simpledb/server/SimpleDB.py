#
#  * The class that configures the system.
#  *
#  * @author Edward Sciore
#
from simpledb.buffer.BufferMgr import BufferMgr
from simpledb.log.LogMgr import LogMgr
from simpledb.metadata.MetadataMgr import MetadataMgr
from simpledb.plan.BasicQueryPlanner import BasicQueryPlanner
from simpledb.plan.BasicUpdatePlanner import BasicUpdatePlanner
from simpledb.plan.Planner import Planner
from simpledb.tx.Transaction import Transaction
from simpledb.util.File import File
from simpledb.file.FileMgr import FileMgr


class SimpleDB(object):
    BLOCK_SIZE = 400
    BUFFER_SIZE = 8
    LOG_FILE = "simpledb.log"

    def __init__(self, *args):
        if len(args) == 3:
            #
            #     * A constructor useful for debugging.
            #     * @param dirname the name of the database directory
            #     * @param blocksize the block size
            #     * @param buffsize the number of buffers
            #
            dirname, blocksize, buffsize = args
            dbDirectory = File(dirname)
            self.fm = FileMgr(dbDirectory, blocksize)
            self.lm = LogMgr(self.fm, SimpleDB.LOG_FILE)
            self.bm = BufferMgr(self.fm, self.lm, buffsize)
        else:
            #
            #     * A simpler constructor for most situations. Unlike the
            #     * 3-arg constructor, it also initializes the metadata tables.
            #     * @param dirname the name of the database directory
            #
            dirname = args[0]
            self.__init__(dirname, self.BLOCK_SIZE, self.BUFFER_SIZE)
            tx = self.newTx()
            isnew = self.fm.isNew()
            if isnew:
                print("creating new database")
            else:
                print("recovering existing database")
                tx.recover()
            self.mdm = MetadataMgr(isnew, tx)
            qp = BasicQueryPlanner(self.mdm)
            up = BasicUpdatePlanner(self.mdm)
            #     QueryPlanner qp = new HeuristicQueryPlanner(mdm);
            #     UpdatePlanner up = new IndexUpdatePlanner(mdm);
            self._planner = Planner(qp, up)
            tx.commit()

    #
    #     * A convenient way for clients to create transactions
    #     * and access the metadata.
    #
    def newTx(self):
        return Transaction(self.fm, self.lm, self.bm)

    def mdMgr(self):
        return self.mdm

    def planner(self):
        return self._planner

    #  These methods aid in debugging
    def fileMgr(self):
        return self.fm

    def logMgr(self):
        return self.lm

    def bufferMgr(self):
        return self.bm
