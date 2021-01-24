#
#  * A class containing static methods,
#  * which estimate the optimal number of buffers
#  * to allocate for a scan.
#  * @author Edward Sciore
#
from simpledb.util.Integer import Integer
import math


class BufferNeeds(object):

    #
    #     * This method considers the various roots
    #     * of the specified output size (in blocks),
    #     * and returns the highest root that is less than
    #     * the number of available buffers.
    #     * <BUG FIX: We reserve a couple of buffers so that we don't run completely out.>
    #     * @param size the size of the output file
    #     * @return the highest number less than the number of available buffers,
    #     that is a root of the plan's output size
    #
    @staticmethod
    def bestRoot(available, size):
        avail = available - 2  # reserve a couple
        if avail <= 1:
            return 1
        k = Integer.MAX_VALUE
        i = 1.0
        while k > avail:
            i += 1
            k = int(math.ceil(math.pow(size, 1 / i)))
        return k

    #
    #     * This method considers the various factors
    #     * of the specified output size (in blocks),
    #     * and returns the highest factor that is less than
    #     * the number of available buffers.
    #     * <BUG FIX: We reserve a couple of buffers so that we don't run completely out.>
    #     * @param size the size of the output file
    #     * @return the highest number less than the number of available buffers,
    #     that is a factor of the plan's output size
    #
    @staticmethod
    def bestFactor(available, size):
        avail = available - 2  # reserve a couple
        if avail <= 1:
            return 1
        k = size
        i = 1.0
        while k > avail:
            i += 1
            k = int(math.ceil(size / i))
        return k
