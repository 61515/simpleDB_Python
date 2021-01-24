
#
#  * Data for the SQL <i>delete</i> statement.
#  * @author Edward Sciore
#


class DeleteData(object):

    #
    #     * Saves the table name and predicate.
    #
    def __init__(self, tblname, pred):
        self.tblname = tblname
        self._pred = pred

    #
    #     * Returns the name of the affected table.
    #     * @return the name of the affected table
    #
    def tableName(self):
        return self.tblname

    #
    #     * Returns the predicate that describes which
    #     * records should be deleted.
    #     * @return the deletion predicate
    #
    def pred(self):
        return self._pred
