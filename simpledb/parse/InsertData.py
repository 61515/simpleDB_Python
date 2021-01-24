
#
#  * Data for the SQL <i>insert</i> statement.
#  * @author Edward Sciore
#


class InsertData(object):

    #
    #     * Saves the table name and the field and value lists.
    #
    def __init__(self, tblname, flds, vals):
        self.tblname = tblname
        self.flds = flds
        self._vals = vals

    #
    #     * Returns the name of the affected table.
    #     * @return the name of the affected table
    #
    def tableName(self):
        return self.tblname

    #
    #     * Returns a list of fields for which
    #     * values will be specified in the new record.
    #     * @return a list of field names
    #
    def fields(self):
        return self.flds

    #
    #     * Returns a list of values for the specified fields.
    #     * There is a one-one correspondence between this
    #     * list of values and the list of fields.
    #     * @return a list of Constant values.
    #
    def vals(self):
        return self._vals
