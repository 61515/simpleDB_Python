from simpledb.file.BlockId import BlockId
from simpledb.util.File import File
from simpledb.util.RandomAccessFile import RandomAccessFile
from simpledb.util.Synchronized import synchronized
import threading
import os


class FileMgr:
    def __init__(self, dbDirectory, blocksize):
        self.lock = threading.Lock()  # 同步锁
        self.dbDirectory = dbDirectory
        self.blocksize = blocksize
        self._isNew = not dbDirectory.exists()
        self.openFiles = {}
        # create the directory if the database is new
        if self._isNew:
            dbDirectory.mkdirs()

        # remove any leftover temporary tables
        for filename in dbDirectory.list():
            if filename.startswith("temp"):
                File(os.path.join(dbDirectory.getname, filename)).delete()

    @synchronized
    def read(self, blk, p):
        try:
            f = self.getFile(blk.fileName())
            f.seek(blk.number() * self.blocksize)
            f.getChannel().read(p.contents())
        except IOError:
            raise RuntimeError("cannot read block " + blk)

    @synchronized
    def write(self, blk, p):
        try:
            f = self.getFile(blk.fileName())
            f.seek(blk.number() * self.blocksize)
            f.getChannel().write(p.contents())
        except IOError:
            raise RuntimeError("cannot write block " + blk)

    @synchronized
    def append(self, filename):
        newblknum = self.length(filename)
        blk = BlockId(filename, newblknum)
        b = bytearray(self.blocksize)
        try:
            f = self.getFile(blk.fileName())
            f.seek(blk.number() * self.blocksize)
            f.write(b)
        except IOError:
            raise RuntimeError("cannot append block" + blk)
        return blk

    def length(self, filename):
        try:
            f = self.getFile(filename)
            return int(f.length() / self.blocksize)
        except IOError:
            raise RuntimeError("cannot access " + filename)

    def isNew(self):
        return self._isNew

    def blockSize(self):
        return self.blocksize

    def getFile(self, filename):
        try:
            f = self.openFiles.get(filename)
            if not f:
                dbTable = File(os.path.join(self.dbDirectory.getname(), filename))
                f = RandomAccessFile(dbTable, "rws")
                self.openFiles[filename] = f
            return f
        except IOError:
            raise IOError
