
#
#  * This class implements all of the methods of the ResultSet interface,
#  * by throwing an exception for each one.
#  * Subclasses (such as SimpleResultSet) can override those methods that
#  * it want to implement.
#  * @author Edward Sciore
#
from simpledb.jdbc.SQLException import SQLException


class ResultSetAdapter:
    def absolute(self, row):
        raise SQLException("operation not implemented")

    def afterLast(self):
        raise SQLException("operation not implemented")

    def beforeFirst(self):
        raise SQLException("operation not implemented")

    def cancelRowUpdates(self):
        raise SQLException("operation not implemented")

    def clearWarnings(self):
        raise SQLException("operation not implemented")

    def close(self):
        raise SQLException("operation not implemented")

    def deleteRow(self):
        raise SQLException("operation not implemented")

    def findColumn(self, columnLabel):
        raise SQLException("operation not implemented")

    def first(self):
        raise SQLException("operation not implemented")

    def getArray(self, columnIndex):
        raise SQLException("operation not implemented")

    def getAsciiStream(self, columnIndex):
        raise SQLException("operation not implemented")

    def getBigDecimal(self, columnIndex):
        raise SQLException("operation not implemented")

    def getBinaryStream(self, columnIndex):
        raise SQLException("operation not implemented")

    def getBlob(self, columnIndex):
        raise SQLException("operation not implemented")

    def getBoolean(self, columnIndex):
        raise SQLException("operation not implemented")

    def getByte(self, columnIndex):
        raise SQLException("operation not implemented")

    def getBytes(self, columnIndex):
        raise SQLException("operation not implemented")

    def getCharacterStream(self, columnIndex):
        raise SQLException("operation not implemented")

    def getClob(self, columnIndex):
        raise SQLException("operation not implemented")

    def getConcurrency(self):
        raise SQLException("operation not implemented")

    def getCursorName(self):
        raise SQLException("operation not implemented")

    def getDate(self, columnIndex):
        raise SQLException("operation not implemented")

    def getDouble(self, columnIndex):
        raise SQLException("operation not implemented")

    def getFetchDirection(self):
        raise SQLException("operation not implemented")

    def getFetchSize(self):
        raise SQLException("operation not implemented")

    def getFloat(self, columnIndex):
        raise SQLException("operation not implemented")

    def getHoldability(self):
        raise SQLException("operation not implemented")

    def getInt(self, columnIndex):
        raise SQLException("operation not implemented")

    def getLong(self, columnIndex):
        raise SQLException("operation not implemented")

    def getMetaData(self):
        raise SQLException("operation not implemented")

    def getNCharacterStream(self, columnIndex):
        raise SQLException("operation not implemented")

    def getNClob(self, columnIndex):
        raise SQLException("operation not implemented")

    def getNString(self, columnIndex):
        raise SQLException("operation not implemented")

    def getObject(self, columnIndex):
        raise SQLException("operation not implemented")

    def getRef(self, columnIndex):
        raise SQLException("operation not implemented")

    def getRow(self):
        raise SQLException("operation not implemented")

    def getRowId(self, columnIndex):
        raise SQLException("operation not implemented")

    def getShort(self, columnIndex):
        raise SQLException("operation not implemented")

    def getSQLXML(self, columnIndex):
        raise SQLException("operation not implemented")

    def getStatement(self):
        raise SQLException("operation not implemented")

    def getString(self, columnIndex):
        raise SQLException("operation not implemented")

    def getTime(self, columnIndex):
        raise SQLException("operation not implemented")

    def getTimestamp(self, columnIndex):
        raise SQLException("operation not implemented")

    def getType(self):
        raise SQLException("operation not implemented")

    def getUnicodeStream(self, columnIndex):
        raise SQLException("operation not implemented")

    def getURL(self, columnIndex):
        raise SQLException("operation not implemented")

    def getWarnings(self):
        raise SQLException("operation not implemented")

    def insertRow(self):
        raise SQLException("operation not implemented")

    def isAfterLast(self):
        raise SQLException("operation not implemented")

    def isBeforeFirst(self):
        raise SQLException("operation not implemented")

    def isClosed(self):
        raise SQLException("operation not implemented")

    def isFirst(self):
        raise SQLException("operation not implemented")

    def isLast(self):
        raise SQLException("operation not implemented")

    def last(self):
        raise SQLException("operation not implemented")

    def moveToCurrentRow(self):
        raise SQLException("operation not implemented")

    def moveToInsertRow(self):
        raise SQLException("operation not implemented")

    def next(self):
        raise SQLException("operation not implemented")

    def previous(self):
        raise SQLException("operation not implemented")

    def refreshRow(self):
        raise SQLException("operation not implemented")

    def relative(self, rows):
        raise SQLException("operation not implemented")

    def rowDeleted(self):
        raise SQLException("operation not implemented")

    def rowInserted(self):
        raise SQLException("operation not implemented")

    def rowUpdated(self):
        raise SQLException("operation not implemented")

    def setFetchDirection(self, direction):
        raise SQLException("operation not implemented")

    def setFetchSize(self, rows):
        raise SQLException("operation not implemented")

    def updateArray(self, columnIndex, x):
        raise SQLException("operation not implemented")

    def updateAsciiStream(self, columnIndex, x):
        raise SQLException("operation not implemented")

    def updateBigDecimal(self, columnIndex, x):
        raise SQLException("operation not implemented")

    def updateBinaryStream(self, columnIndex, x):
        raise SQLException("operation not implemented")

    def updateBlob(self, columnIndex, x):
        raise SQLException("operation not implemented")

    def updateBoolean(self, columnIndex, x):
        raise SQLException("operation not implemented")

    def updateByte(self, columnIndex, x):
        raise SQLException("operation not implemented")

    def updateBytes(self, columnIndex, x):
        raise SQLException("operation not implemented")

    def updateCharacterStream(self, columnIndex, x):
        raise SQLException("operation not implemented")

    def updateClob(self, columnIndex, x):
        raise SQLException("operation not implemented")

    def updateDate(self, columnIndex, x):
        raise SQLException("operation not implemented")

    def updateDouble(self, columnIndex, x):
        raise SQLException("operation not implemented")

    def updateFloat(self, columnIndex, x):
        raise SQLException("operation not implemented")

    def updateInt(self, columnIndex, x):
        raise SQLException("operation not implemented")

    def updateLong(self, columnIndex, x):
        raise SQLException("operation not implemented")

    def updateNCharacterStream(self, columnIndex, x):
        raise SQLException("operation not implemented")

    def updateNClob(self, columnIndex, nclob):
        raise SQLException("operation not implemented")

    def updateNString(self, columnIndex, nstring):
        raise SQLException("operation not implemented")

    def updateNull(self, columnIndex):
        raise SQLException("operation not implemented")

    def updateObject(self, columnIndex, x):
        raise SQLException("operation not implemented")

    def updateRef(self, columnIndex, x):
        raise SQLException("operation not implemented")

    def updateRow(self):
        raise SQLException("operation not implemented")

    def updateShort(self, columnIndex, x):
        raise SQLException("operation not implemented")

    def updateSQLXML(self, columnIndex, x):
        raise SQLException("operation not implemented")

    def updateString(self, columnIndex, x):
        raise SQLException("operation not implemented")

    def updateTime(self, columnIndex, x):
        raise SQLException("operation not implemented")

    def updateTimestamp(self, columnIndex, x):
        raise SQLException("operation not implemented")

    def wasNull(self):
        raise SQLException("operation not implemented")

    def isWrapperFor(self, iface):
        raise SQLException("operation not implemented")

    def unwrap(self, iface):
        raise SQLException("operation not implemented")
