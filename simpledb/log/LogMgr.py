from simpledb.file.BlockId import BlockId
from simpledb.file.Page import Page
from simpledb.log.LogIterator import LogIterator
from simpledb.util.Integer import Integer
from simpledb.util.Synchronized import synchronized
import threading


class LogMgr:
    # Creates the manager for the specified log file.
    # If the log file does not yet exist, it is created
    # with an empty first block.
    # @param FileMgr the file manager
    # @param logfile the name of the log file
    def __init__(self, fm, logfile):
        self.fm = fm
        self.logfile = logfile
        b = bytearray(fm.blockSize())
        self.logpage = Page(b)
        logsize = fm.length(logfile)
        if logsize == 0:
            self.currentblk = self.appendNewBlock()
        else:
            self.currentblk = BlockId(logfile, logsize - 1)
            self.fm.read(self.currentblk, self.logpage)
        self.latestLSN = 0
        self.lastSavedLSN = 0
        self.lock = threading.Lock()  # 同步锁

    # Ensures that the log record corresponding to the
    # specified LSN has been written to disk.
    # All earlier log records will also be written to disk.
    # @param lsn the LSN of a log record
    def flush(self, *args):
        if len(args) == 1:
            lsn = args[0]
            if lsn >= self.lastSavedLSN:
                self.flush()
        # Write the buffer to the log file.
        elif len(args) == 0:
            self.fm.write(self.currentblk, self.logpage)
            self.lastSavedLSN = self.latestLSN

    def iterator(self):
        self.flush()
        return LogIterator(self.fm, self.currentblk)

    # Appends a log record to the log buffer.
    # The record consists of an arbitrary array of bytes.
    # Log records are written right to left in the buffer.
    # The size of the record is written before the bytes.
    # The beginning of the buffer contains the location
    # of the last-written record (the "boundary").
    # Storing the records backwards makes it easy to read
    # them in reverse order.
    # @param logrec a byte buffer containing the bytes.
    # @return the LSN of the final value
    @synchronized
    def append(self, logrec):
        boundary = self.logpage.getInt(0)
        recsize = len(logrec)
        bytesneeded = recsize + Integer.BYTES
        if boundary - bytesneeded < Integer.BYTES:  # the log record doesn't fit,
            self.flush()  # so move to the next block.
            self.currentblk = self.appendNewBlock()
            boundary = self.logpage.getInt(0)

        recpos = boundary - bytesneeded
        self.logpage.setBytes(recpos, logrec)
        self.logpage.setInt(0, recpos)  # the new boundary
        self.latestLSN += 1
        return self.latestLSN

    # Initialize the bytebuffer and append it to the log file.
    def appendNewBlock(self):
        blk = self.fm.append(self.logfile)
        self.logpage.setInt(0, self.fm.blockSize())
        self.fm.write(blk, self.logpage)
        return blk
