#
#  * This interface contains methods to traverse an index.
#  * @author Edward Sciore
#  *
#
import abc


class Index(abc.ABCMeta):
    #
    #     * Positions the index before the first record
    #     * having the specified search key.
    #     * @param searchkey the search key value.
    #
    @abc.abstractmethod
    def beforeFirst(self, searchkey):
        """ method beforeFirst """

    #
    #     * Moves the index to the next record having the
    #     * search key specified in the beforeFirst method.
    #     * Returns false if there are no more such index records.
    #     * @return false if no other index records have the search key.
    #
    @abc.abstractmethod
    def next(self):
        """ method next """

    #
    #     * Returns the dataRID value stored in the current index record.
    #     * @return the dataRID stored in the current index record.
    #
    @abc.abstractmethod
    def getDataRid(self):
        """ method getDataRid """

    #
    #     * Inserts an index record having the specified
    #     * dataval and dataRID values.
    #     * @param dataval the dataval in the new index record.
    #     * @param datarid the dataRID in the new index record.
    #
    @abc.abstractmethod
    def insert(self, dataval, datarid):
        """ method insert """

    #
    #     * Deletes the index record having the specified
    #     * dataval and dataRID values.
    #     * @param dataval the dataval of the deleted index record
    #     * @param datarid the dataRID of the deleted index record
    #
    @abc.abstractmethod
    def delete(self, dataval, datarid):
        """ method delete """

    #
    #     * Closes the index.
    #
    @abc.abstractmethod
    def close(self):
        """ method close """
