
#
#  * Data for the SQL <i>select</i> statement.
#  * @author Edward Sciore
#


class QueryData(object):

    #
    #     * Saves the field and table list and predicate.
    #
    def __init__(self, fields, tables, pred):
        self._fields = fields
        self._tables = tables
        self._pred = pred

    #
    #     * Returns the fields mentioned in the select clause.
    #     * @return a list of field names
    #
    def fields(self):
        return self._fields

    #
    #     * Returns the tables mentioned in the from clause.
    #     * @return a collection of table names
    #
    def tables(self):
        return self._tables

    #
    #     * Returns the predicate that describes which
    #     * records should be in the output table.
    #     * @return the query predicate
    #
    def pred(self):
        return self._pred

    def __str__(self):
        result = "select "
        for fldname in self._fields:
            result += fldname + ", "
        result = result[0: len(result) - 2]  # remove final comma
        result += " from "
        for tblname in self._tables:
            result += tblname + ", "
        result = result[0: len(result) - 2]  # remove final comma
        predstring = self._pred.__str__()
        if not predstring == "":
            result += " where " + predstring
        return result
