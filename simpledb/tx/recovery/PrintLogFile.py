from simpledb.file.BlockId import BlockId
from simpledb.file.Page import Page
from simpledb.log.LogMgr import LogMgr
from simpledb.tx.recovery.LogRecord import LogRecord
from simpledb.util.File import File
from simpledb.file.FileMgr import FileMgr


class PrintLogFile(object):
    @classmethod
    def main(cls, args):
        # db = SimpleDB("studentdb", 400, 8)
        fm = FileMgr(File("recoverytest"), 400)
        lm = LogMgr(fm, "simpledb.log")

        filename = "simpledb.log"
        lastblock = fm.length(filename) - 1
        blk = BlockId(filename, lastblock)
        p = Page(fm.blockSize())
        fm.read(blk, p)
        iterator = lm.iterator()
        while iterator.hasNext():
            byte_array = iterator.next()
            rec = LogRecord.createLogRecord(byte_array)
            print(rec)


if __name__ == '__main__':
    import sys
    PrintLogFile.main(sys.argv)
