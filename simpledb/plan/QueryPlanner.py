
#
#  * The interface implemented by planners for
#  * the SQL select statement.
#  * @author Edward Sciore
#  *
#
import abc


class QueryPlanner(metaclass=abc.ABCMeta):
    #
    #     * Creates a plan for the parsed query.
    #     * @param data the parsed representation of the query
    #     * @param tx the calling transaction
    #     * @return a plan for that query
    #
    @abc.abstractmethod
    def createPlan(self, data, tx):
        """ method createPlan """
