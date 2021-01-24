#
#  * An individual buffer. A databuffer wraps a page
#  * and stores information about its status,
#  * such as the associated disk block,
#  * the number of times the buffer has been pinned,
#  * whether its contents have been modified,
#  * and if so, the id and lsn of the modifying transaction.
#  * @author Edward Sciore
#
from simpledb.file.Page import Page


class Buffer(object):

    def __init__(self, fm, lm):
        self.fm = fm
        self.lm = lm
        self._contents = Page(fm.blockSize())
        self.blk = None
        self.pins = 0
        self.txnum = -1
        self.lsn = -1

    def contents(self):
        return self._contents

    #
    #     * Returns a reference to the disk block
    #     * allocated to the buffer.
    #     * @return a reference to a disk block
    #
    def block(self):
        return self.blk

    def setModified(self, txnum, lsn):
        self.txnum = txnum
        if lsn >= 0:
            self.lsn = lsn

    #
    #     * Return true if the buffer is currently pinned
    #     * (that is, if it has a nonzero pin count).
    #     * @return true if the buffer is pinned
    #
    def isPinned(self):
        return self.pins > 0

    def modifyingTx(self):
        return self.txnum

    #
    #     * Reads the contents of the specified block into
    #     * the contents of the buffer.
    #     * If the buffer was dirty, then its previous contents
    #     * are first written to disk.
    #     * @param b a reference to the data block
    #
    def assignToBlock(self, b):
        self.flush()
        self.blk = b
        self.fm.read(self.blk, self._contents)
        self.pins = 0

    #
    #     * Write the buffer to its disk block if it is dirty.
    #
    def flush(self):
        if self.txnum >= 0:
            self.lm.flush(self.lsn)
            self.fm.write(self.blk, self._contents)
            self.txnum = -1

    #
    #     * Increase the buffer's pin count.
    #
    def pin(self):
        self.pins += 1

    #
    #     * Decrease the buffer's pin count.
    #
    def unpin(self):
        self.pins -= 1
