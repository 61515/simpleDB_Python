from simpledb.file.Page import Page
from simpledb.log.LogMgr import LogMgr
from simpledb.util.File import File
from simpledb.util.Integer import Integer
from simpledb.file.FileMgr import FileMgr


class LogTest(object):
    fm = FileMgr(File("filetest"), 400)
    lm = LogMgr(fm, "logfile")

    @classmethod
    def main(cls, args):
        # cls.createRecords(1, 35)
        # cls.printLogRecords("The log file now has these records:")
        # db = SimpleDB("logtest", 400, 8)
        # cls.lm = db.logMgr()
        cls.printLogRecords("The initial empty log file:")
        # print an empty log file
        print("done")
        cls.createRecords(1, 35)
        cls.printLogRecords("The log file now has these records:")
        cls.createRecords(36, 70)
        cls.lm.flush(65)
        cls.printLogRecords("The log file now has these records:")

    @classmethod
    def printLogRecords(cls, msg):
        print(msg)
        iterator = cls.lm.iterator()
        while iterator.hasNext():
            rec = iterator.next()
            p = Page(rec)
            s = p.getString(0)
            npos = Page.maxLength(len(s))
            val = p.getInt(npos)
            print("[" + s + ", " + str(val) + "]")
        print()

    @classmethod
    def createRecords(cls, start, end):
        print("Creating records: ")
        for i in range(start, end + 1):
            rec = cls.createLogRecord("record" + str(i), i + 100)
            lsn = cls.lm.append(rec)
            print(str(lsn) + " ", end='')
        print()

    #  Create a log record having two values: a string and an integer.
    @classmethod
    def createLogRecord(cls, s, n):
        spos = 0
        npos = spos + Page.maxLength(len(s))
        b = bytearray(npos + Integer.BYTES)
        p = Page(b)
        p.setString(spos, s)
        p.setInt(npos, n)
        return b


if __name__ == '__main__':
    import sys
    LogTest.main(sys.argv)
