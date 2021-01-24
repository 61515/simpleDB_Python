#
#  * A StatInfo object holds three pieces of
#  * statistical information about a table:
#  * the number of blocks, the number of records,
#  * and the number of distinct values for each field.
#  * @author Edward Sciore
#


class StatInfo(object):

    #
    #     * Create a StatInfo object.
    #     * Note that the number of distinct values is not
    #     * passed into the constructor.
    #     * The object fakes this value.
    #     * @param numblocks the number of blocks in the table
    #     * @param numrecs the number of records in the table
    #
    def __init__(self, numblocks, numrecs):
        self.numBlocks = numblocks
        self.numRecs = numrecs

    #
    #     * Return the estimated number of blocks in the table.
    #     * @return the estimated number of blocks in the table
    #
    def blocksAccessed(self):
        return self.numBlocks

    #
    #     * Return the estimated number of records in the table.
    #     * @return the estimated number of records in the table
    #
    def recordsOutput(self):
        return self.numRecs

    #
    #     * Return the estimated number of distinct values
    #     * for the specified field.
    #     * This estimate is a complete guess, because doing something
    #     * reasonable is beyond the scope of this system.
    #     * @param fldname the name of the field
    #     * @return a guess as to the number of distinct field values
    #
    def distinctValues(self, fldname):
        return 1 + (self.numRecs / 3)
