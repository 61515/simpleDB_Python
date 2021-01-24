#
#  * A directory entry has two components: the number of the child block,
#  * and the dataval of the first record in that block.
#  * @author Edward Sciore
#


class DirEntry(object):

    #
    #     * Creates a new entry for the specified dataval and block number.
    #     * @param dataval the dataval
    #     * @param blocknum the block number
    #
    def __init__(self, dataval, blocknum):
        self.dataval = dataval
        self.blocknum = blocknum

    #
    #     * Returns the dataval component of the entry
    #     * @return the dataval component of the entry
    #
    def dataVal(self):
        return self.dataval

    #
    #     * Returns the block number component of the entry
    #     * @return the block number component of the entry
    #
    def blockNumber(self):
        return self.blocknum
