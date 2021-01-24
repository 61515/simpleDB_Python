
#
#  * This class implements all of the methods of the ResultSetMetaData interface,
#  * by throwing an exception for each one.
#  * Subclasses (such as SimpleMetaData) can override those methods that
#  * it want to implement.
#  * @author Edward Sciore
#
from simpledb.jdbc.SQLException import SQLException


class ResultSetMetaDataAdapter:
    def getCatalogName(self, column):
        raise SQLException("operation not implemented")

    def getColumnClassName(self, column):
        raise SQLException("operation not implemented")

    def getColumnCount(self):
        raise SQLException("operation not implemented")

    def getColumnDisplaySize(self, column):
        raise SQLException("operation not implemented")

    def getColumnLabel(self, column):
        raise SQLException("operation not implemented")

    def getColumnName(self, column):
        raise SQLException("operation not implemented")

    def getColumnType(self, column):
        raise SQLException("operation not implemented")

    def getColumnTypeName(self, column):
        raise SQLException("operation not implemented")

    def getPrecision(self, column):
        raise SQLException("operation not implemented")

    def getScale(self, column):
        raise SQLException("operation not implemented")

    def getSchemaName(self, column):
        raise SQLException("operation not implemented")

    def getTableName(self, column):
        raise SQLException("operation not implemented")

    def isAutoIncrement(self, column):
        raise SQLException("operation not implemented")

    def isCaseSensitive(self, column):
        raise SQLException("operation not implemented")

    def isCurrency(self, column):
        raise SQLException("operation not implemented")

    def isDefinitelyWritable(self, column):
        raise SQLException("operation not implemented")

    def isNullable(self, column):
        raise SQLException("operation not implemented")

    def isReadOnly(self, column):
        raise SQLException("operation not implemented")

    def isSearchable(self, column):
        raise SQLException("operation not implemented")

    def isSigned(self, column):
        raise SQLException("operation not implemented")

    def isWritable(self, column):
        raise SQLException("operation not implemented")

    def isWrapperFor(self, iface):
        raise SQLException("operation not implemented")

    def unwrap(self, iface):
        raise SQLException("operation not implemented")
