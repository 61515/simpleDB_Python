
#
#  * The embedded implementation of ResultSetMetaData.
#  * @author Edward Sciore
#
from simpledb.jdbc.ResultSetMetaDataAdapter import ResultSetMetaDataAdapter
from simpledb.util.Types import INTEGER


class EmbeddedMetaData(ResultSetMetaDataAdapter):

    #
    #     * Creates a metadata object that wraps the specified schema.
    #     * The method also creates a list to hold the schema's
    #     * collection of field names,
    #     * so that the fields can be accessed by position.
    #     * @param sch the schema
    #
    def __init__(self, sch):
        super(EmbeddedMetaData, self).__init__()
        self.sch = sch

    #
    #     * Returns the size of the field list.
    #
    def getColumnCount(self):
        return len(self.sch.fields())

    #
    #     * Returns the field name for the specified column number.
    #     * In JDBC, column numbers start with 1, so the field
    #     * is taken from position (column-1) in the list.
    #
    def getColumnName(self, column):
        return self.sch.fields()[column - 1]

    #
    #     * Returns the type of the specified column.
    #     * The method first finds the name of the field in that column,
    #     * and then looks up its type in the schema.
    #
    def getColumnType(self, column):
        fldname = self.getColumnName(column)
        return self.sch.type(fldname)

    #
    #     * Returns the number of characters required to display the
    #     * specified column.
    #     * For a string-type field, the method simply looks up the
    #     * field's length in the schema and returns that.
    #     * For an int-type field, the method needs to decide how
    #     * large integers can be.
    #     * Here, the method arbitrarily chooses 6 characters,
    #     * which means that integers over 999,999 will
    #     * probably get displayed improperly.
    #
    def getColumnDisplaySize(self, column):
        fldname = self.getColumnName(column)
        fldtype = self.sch.type(fldname)
        fldlength = 6 if (fldtype == INTEGER) else self.sch.length(fldname)
        return max(len(fldname), fldlength) + 1
