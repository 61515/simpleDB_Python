#
#  * This class implements all of the methods of the Statement interface,
#  * by throwing an exception for each one.
#  * Subclasses (such as SimpleStatement) can override those methods that
#  * it want to implement.
#  * @author Edward Sciore
#
from simpledb.jdbc.SQLException import SQLException


class StatementAdapter:
    def addBatch(self, sql):
        raise SQLException("operation not implemented")

    def cancel(self):
        raise SQLException("operation not implemented")

    def clearBatch(self):
        raise SQLException("operation not implemented")

    def clearWarnings(self):
        raise SQLException("operation not implemented")

    def close(self):
        raise SQLException("operation not implemented")

    def execute(self, sql):
        raise SQLException("operation not implemented")

    def executeBatch(self):
        raise SQLException("operation not implemented")

    def executeQuery(self, sql):
        raise SQLException("operation not implemented")

    def executeUpdate(self, sql):
        raise SQLException("operation not implemented")

    def getConnection(self):
        raise SQLException("operation not implemented")

    def getFetchDirection(self):
        raise SQLException("operation not implemented")

    def getFetchSize(self):
        raise SQLException("operation not implemented")

    def getGeneratedKeys(self):
        raise SQLException("operation not implemented")

    def getMaxFieldSize(self):
        raise SQLException("operation not implemented")

    def getMaxRows(self):
        raise SQLException("operation not implemented")

    def getMoreResults(self):
        raise SQLException("operation not implemented")

    def getQueryTimeout(self):
        raise SQLException("operation not implemented")

    def getResultSet(self):
        raise SQLException("operation not implemented")

    def getResultSetConcurrency(self):
        raise SQLException("operation not implemented")

    def getResultSetHoldability(self):
        raise SQLException("operation not implemented")

    def getResultSetType(self):
        raise SQLException("operation not implemented")

    def getUpdateCount(self):
        raise SQLException("operation not implemented")

    def getWarnings(self):
        raise SQLException("operation not implemented")

    def isClosed(self):
        raise SQLException("operation not implemented")

    def isPoolable(self):
        raise SQLException("operation not implemented")

    def setCursorName(self, name):
        raise SQLException("operation not implemented")

    def setEscapeProcessing(self, enable):
        """ method setEscapeProcessing """

    def setFetchDirection(self, direction):
        """ method setFetchDirection """

    def setFetchSize(self, rows):
        raise SQLException("operation not implemented")

    def setMaxFieldSize(self, _max):
        raise SQLException("operation not implemented")

    def setMaxRows(self, _max):
        raise SQLException("operation not implemented")

    def setPoolable(self, poolable):
        raise SQLException("operation not implemented")

    def setQueryTimeout(self, seconds):
        raise SQLException("operation not implemented")

    def isWrapperFor(self, iface):
        raise SQLException("operation not implemented")

    def unwrap(self, iface):
        raise SQLException("operation not implemented")

    def closeOnCompletion(self):
        raise SQLException("operation not implemented")

    def isCloseOnCompletion(self):
        raise SQLException("operation not implemented")
