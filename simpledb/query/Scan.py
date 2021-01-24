#
#  * The interface will be implemented by each query scan.
#  * There is a Scan class for each relational
#  * algebra operator.
#  * @author Edward Sciore
#
import abc


class Scan(metaclass=abc.ABCMeta):
    #
    #     * Position the scan before its first record. A
    #     * subsequent call to next() will return the first record.
    #
    @abc.abstractmethod
    def beforeFirst(self):
        """ method beforeFirst """

    #
    #     * Move the scan to the next record.
    #     * @return false if there is no next record
    #
    @abc.abstractmethod
    def next(self):
        """ method next """

    #
    #     * Return the value of the specified integer field
    #     * in the current record.
    #     * @param fldname the name of the field
    #     * @return the field's integer value in the current record
    #
    @abc.abstractmethod
    def getInt(self, fldname):
        """ method getInt """

    #
    #     * Return the value of the specified string field
    #     * in the current record.
    #     * @param fldname the name of the field
    #     * @return the field's string value in the current record
    #
    @abc.abstractmethod
    def getString(self, fldname):
        """ method getString """

    #
    #     * Return the value of the specified field in the current record.
    #     * The value is expressed as a Constant.
    #     * @param fldname the name of the field
    #     * @return the value of that field, expressed as a Constant.
    #
    @abc.abstractmethod
    def getVal(self, fldname):
        """ method getVal """

    #
    #     * Return true if the scan has the specified field.
    #     * @param fldname the name of the field
    #     * @return true if the scan has that field
    #
    @abc.abstractmethod
    def hasField(self, fldname):
        """ method hasField """

    #
    #     * Close the scan and its subscans, if any.
    #
    @abc.abstractmethod
    def close(self):
        """ method close """
