#
#  * The interface implemented by all updateable scans.
#  * @author Edward Sciore
#
from abc import ABC
from simpledb.query.Scan import Scan
import abc


class UpdateScan(Scan, ABC):
    #
    #     * Modify the field value of the current record.
    #     * @param fldname the name of the field
    #     * @param val the new value, expressed as a Constant
    #
    @abc.abstractmethod
    def setVal(self, fldname, val):
        """ method setVal """

    #
    #     * Modify the field value of the current record.
    #     * @param fldname the name of the field
    #     * @param val the new integer value
    #
    @abc.abstractmethod
    def setInt(self, fldname, val):
        """ method setInt """

    #
    #     * Modify the field value of the current record.
    #     * @param fldname the name of the field
    #     * @param val the new string value
    #
    @abc.abstractmethod
    def setString(self, fldname, val):
        """ method setString """

    #
    #     * Insert a new record somewhere in the scan.
    #
    @abc.abstractmethod
    def insert(self):
        """ method insert """

    #
    #     * Delete the current record from the scan.
    #
    @abc.abstractmethod
    def delete(self):
        """ method delete """

    #
    #     * Return the id of the current record.
    #     * @return the id of the current record
    #
    @abc.abstractmethod
    def getRid(self):
        """ method getRid """

    #
    #     * Position the scan so that the current record has
    #     * the specified id.
    #     * @param rid the id of the desired record
    #
    @abc.abstractmethod
    def moveToRid(self, rid):
        """ method moveToRid """
