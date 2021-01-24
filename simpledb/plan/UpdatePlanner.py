
#
#  * The interface implemented by the planners
#  * for SQL insert, delete, and modify statements.
#  * @author Edward Sciore
#
import abc


class UpdatePlanner(metaclass=abc.ABCMeta):

    #
    #     * Executes the specified insert statement, and
    #     * returns the number of affected records.
    #     * @param data the parsed representation of the insert statement
    #     * @param tx the calling transaction
    #     * @return the number of affected records
    #
    @abc.abstractmethod
    def executeInsert(self, data, tx):
        """ method executeInsert """

    #
    #     * Executes the specified delete statement, and
    #     * returns the number of affected records.
    #     * @param data the parsed representation of the delete statement
    #     * @param tx the calling transaction
    #     * @return the number of affected records
    #
    @abc.abstractmethod
    def executeDelete(self, data, tx):
        """ method executeDelete """

    #
    #     * Executes the specified modify statement, and
    #     * returns the number of affected records.
    #     * @param data the parsed representation of the modify statement
    #     * @param tx the calling transaction
    #     * @return the number of affected records
    #
    @abc.abstractmethod
    def executeModify(self, data, tx):
        """ method executeModify """

    #
    #     * Executes the specified create table statement, and
    #     * returns the number of affected records.
    #     * @param data the parsed representation of the create table statement
    #     * @param tx the calling transaction
    #     * @return the number of affected records
    #
    @abc.abstractmethod
    def executeCreateTable(self, data, tx):
        """ method executeCreateTable """

    #
    #     * Executes the specified create view statement, and
    #     * returns the number of affected records.
    #     * @param data the parsed representation of the create view statement
    #     * @param tx the calling transaction
    #     * @return the number of affected records
    #
    @abc.abstractmethod
    def executeCreateView(self, data, tx):
        """ method executeCreateView """

    #
    #     * Executes the specified create index statement, and
    #     * returns the number of affected records.
    #     * @param data the parsed representation of the create index statement
    #     * @param tx the calling transaction
    #     * @return the number of affected records
    #
    @abc.abstractmethod
    def executeCreateIndex(self, data, tx):
        """ method executeCreateIndex """
