
#
#  * Data for the SQL <i>create table</i> statement.
#  * @author Edward Sciore
#


class CreateTableData(object):

    #
    #     * Saves the table name and schema.
    #
    def __init__(self, tblname, sch):
        self.tblname = tblname
        self.sch = sch

    #
    #     * Returns the name of the new table.
    #     * @return the name of the new table
    #
    def tableName(self):
        return self.tblname

    #
    #     * Returns the schema of the new table.
    #     * @return the schema of the new table
    #
    def newSchema(self):
        return self.sch
