#
#  * An identifier for a record within a file.
#  * A RID consists of the block number in the file,
#  * and the location of the record in that block.
#  * @author Edward Sciore
#


class RID(object):

    #
    #    * Create a RID for the record having the
    #    * specified location in the specified block.
    #    * @param blknum the block number where the record lives
    #    * @param slot the record's loction
    #
    def __init__(self, blknum, slot):
        self.blknum = blknum
        self._slot = slot

    #
    #    * Return the block number associated with this RID.
    #    * @return the block number
    #
    def blockNumber(self):
        return self.blknum

    #
    #    * Return the slot associated with this RID.
    #    * @return the slot
    #
    def slot(self):
        return self._slot

    def __eq__(self, other):
        r = RID(other)
        return self.blknum == r.blknum and self._slot == r._slot

    def __str__(self):
        return "[" + str(self.blknum) + ", " + str(self._slot) + "]"
