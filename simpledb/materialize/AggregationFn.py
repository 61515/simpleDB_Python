#
#  * The interface implemented by aggregation functions.
#  * Aggregation functions are used by the <i>groupby</i> operator.
#  * @author Edward Sciore
#
import abc


class AggregationFn(abc.ABCMeta):

    #
    #     * Use the current record of the specified scan
    #     * to be the first record in the group.
    #     * @param s the scan to aggregate over.
    #
    @abc.abstractmethod
    def processFirst(self, s):
        """ method processFirst """

    #
    #     * Use the current record of the specified scan
    #     * to be the next record in the group.
    #     * @param s the scan to aggregate over.
    #
    @abc.abstractmethod
    def processNext(self, s):
        """ method processNext """

    #
    #     * Return the name of the new aggregation field.
    #     * @return the name of the new aggregation field
    #
    @abc.abstractmethod
    def fieldName(self):
        """ method fieldName """

    #
    #     * Return the computed aggregation value.
    #     * @return the computed aggregation value
    #
    @abc.abstractmethod
    def value(self):
        """ method value """
