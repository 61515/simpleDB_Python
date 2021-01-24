
#
#  * The interface implemented by each query plan.
#  * There is a Plan class for each relational algebra operator.
#  * @author Edward Sciore
#  *
#
import abc


class Plan(metaclass=abc.ABCMeta):
    #
    #     * Opens a scan corresponding to this plan.
    #     * The scan will be positioned before its first record.
    #     * @return a scan
    #
    @abc.abstractmethod
    def open(self):
        """ method open """

    #
    #     * Returns an estimate of the number of block accesses
    #     * that will occur when the scan is read to completion.
    #     * @return the estimated number of block accesses
    #
    @abc.abstractmethod
    def blocksAccessed(self):
        """ method blocksAccessed """

    #
    #     * Returns an estimate of the number of records
    #     * in the query's output table.
    #     * @return the estimated number of output records
    #
    @abc.abstractmethod
    def recordsOutput(self):
        """ method recordsOutput """

    #
    #     * Returns an estimate of the number of distinct values
    #     * for the specified field in the query's output table.
    #     * @param fldname the name of a field
    #     * @return the estimated number of distinct field values in the output
    #
    @abc.abstractmethod
    def distinctValues(self, fldname):
        """ method distinctValues """

    #
    #     * Returns the schema of the query.
    #     * @return the query's schema
    #
    @abc.abstractmethod
    def schema(self):
        """ method schema """
