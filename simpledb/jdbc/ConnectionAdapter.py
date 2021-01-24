#
#  * This class implements all of the methods of the Connection interface,
#  * by throwing an exception for each one.
#  * Subclasses (such as SimpleConnection) can override those methods that
#  * it want to implement.
#  * @author Edward Sciore
#
from simpledb.jdbc.SQLException import SQLException


class ConnectionAdapter:
    def clearWarnings(self):
        raise SQLException("operation not implemented")

    def close(self):
        raise SQLException("operation not implemented")

    def commit(self):
        raise SQLException("operation not implemented")

    def createArrayOf(self, typeName, elements):
        raise SQLException("operation not implemented")

    def createBlob(self):
        raise SQLException("operation not implemented")

    def createClob(self):
        raise SQLException("operation not implemented")

    def createNClob(self):
        raise SQLException("operation not implemented")

    def createSQLXML(self):
        raise SQLException("operation not implemented")

    def createStatement(self):
        raise SQLException("operation not implemented")

    def createStruct(self, typeName, attributes):
        raise SQLException("operation not implemented")

    def getAutoCommit(self):
        raise SQLException("operation not implemented")

    def getCatalog(self):
        raise SQLException("operation not implemented")

    def getClientInfo(self, name):
        raise SQLException("operation not implemented")

    def getHoldability(self):
        raise SQLException("operation not implemented")

    def getMetaData(self):
        raise SQLException("operation not implemented")

    def getTransactionIsolation(self):
        raise SQLException("operation not implemented")

    def getTypeMap(self):
        raise SQLException("operation not implemented")

    def getWarnings(self):
        raise SQLException("operation not implemented")

    def isClosed(self):
        raise SQLException("operation not implemented")

    def isReadOnly(self):
        raise SQLException("operation not implemented")

    def isValid(self, timeout):
        raise SQLException("operation not implemented")

    def nativeSQL(self, sql):
        raise SQLException("operation not implemented")

    def prepareCall(self, sql):
        raise SQLException("operation not implemented")

    def prepareStatement(self, sql):
        raise SQLException("operation not implemented")

    def releaseSavepoint(self, savepoint):
        raise SQLException("operation not implemented")

    def rollback(self):
        raise SQLException("operation not implemented")

    def setAutoCommit(self, autoCommit):
        raise SQLException("operation not implemented")

    def setCatalog(self, catalog):
        raise SQLException("operation not implemented")

    def setClientInfo(self, name, value):
        """ method setClientInfo """

    def setHoldability(self, holdability):
        raise SQLException("operation not implemented")

    def setReadOnly(self, readOnly):
        raise SQLException("operation not implemented")

    def setSavepoint(self):
        raise SQLException("operation not implemented")

    def setTransactionIsolation(self, level):
        raise SQLException("operation not implemented")

    def setTypeMap(self, _map):
        raise SQLException("operation not implemented")

    def isWrapperFor(self, iface):
        raise SQLException("operation not implemented")

    def unwrap(self, iface):
        raise SQLException("operation not implemented")

    def abort(self, executor):
        raise SQLException("operation not implemented")

    def getNetworkTimeout(self):
        raise SQLException("operation not implemented")

    def getSchema(self):
        raise SQLException("operation not implemented")

    def setNetworkTimeout(self, executor, milliseconds):
        raise SQLException("operation not implemented")

    def setSchema(self, schema):
        raise SQLException("operation not implemented")
