
#
#  * Data for the SQL <i>update</i> statement.
#  * @author Edward Sciore
#


class ModifyData(object):

    #
    #     * Saves the table name, the modified field and its new value, and the predicate.
    #
    def __init__(self, tblname, fldname, newval, pred):
        self.tblname = tblname
        self.fldname = fldname
        self.newval = newval
        self._pred = pred

    #
    #     * Returns the name of the affected table.
    #     * @return the name of the affected table
    #
    def tableName(self):
        return self.tblname

    #
    #     * Returns the field whose values will be modified
    #     * @return the name of the target field
    #
    def targetField(self):
        return self.fldname

    #
    #     * Returns an expression.
    #     * Evaluating this expression for a record produces
    #     * the value that will be stored in the record's target field.
    #     * @return the target expression
    #
    def newValue(self):
        return self.newval

    #
    #     * Returns the predicate that describes which
    #     * records should be modified.
    #     * @return the modification predicate
    #
    def pred(self):
        return self._pred
