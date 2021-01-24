#
#  * The parser for the <i>create index</i> statement.
#  * @author Edward Sciore
#


class CreateIndexData(object):

    #
    #     * Saves the table and field names of the specified index.
    #
    def __init__(self, idxname, tblname, fldname):
        self.idxname = idxname
        self.tblname = tblname
        self.fldname = fldname

    #
    #     * Returns the name of the index.
    #     * @return the name of the index
    #
    def indexName(self):
        return self.idxname

    #
    #     * Returns the name of the indexed table.
    #     * @return the name of the indexed table
    #
    def tableName(self):
        return self.tblname

    #
    #     * Returns the name of the indexed field.
    #     * @return the name of the indexed field
    #
    def fieldName(self):
        return self.fldname
