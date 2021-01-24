#
#  * Manage the transaction's currently-pinned buffers.
#  * @author Edward Sciore
#


class BufferList(object):

    def __init__(self, bm):
        self.bm = bm
        self.buffers = {}
        self.pins = []

    #
    #     * Return the buffer pinned to the specified block.
    #     * The method returns null if the transaction has not
    #     * pinned the block.
    #     * @param blk a reference to the disk block
    #     * @return the buffer pinned to that block
    #
    def getBuffer(self, blk):
        return self.buffers.get(blk)

    #
    #     * Pin the block and keep track of the buffer internally.
    #     * @param blk a reference to the disk block
    #
    def pin(self, blk):
        buff = self.bm.pin(blk)
        self.buffers[blk] = buff
        self.pins.append(blk)

    #
    #     * Unpin the specified block.
    #     * @param blk a reference to the disk block
    #
    def unpin(self, blk):
        buff = self.buffers.get(blk)
        self.bm.unpin(buff)
        self.pins.remove(blk)

        if blk not in self.pins:
            self.buffers.pop(blk)

    #
    #     * Unpin any buffers still pinned by this transaction.
    #
    def unpinAll(self):
        for blk in self.pins:
            buff = self.buffers.get(blk)
            self.bm.unpin(buff)
        self.buffers.clear()
        self.pins.clear()
