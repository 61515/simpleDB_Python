#
#  * Data for the SQL <i>create view</i> statement.
#  * @author Edward Sciore
#


class CreateViewData(object):

    #
    #     * Saves the view name and its definition.
    #
    def __init__(self, viewname, qrydata):
        self.viewname = viewname
        self.qrydata = qrydata

    #
    #     * Returns the name of the new view.
    #     * @return the name of the new view
    #
    def viewName(self):
        return self.viewname

    #
    #     * Returns the definition of the new view.
    #     * @return the definition of the new view
    #
    def viewDef(self):
        return self.qrydata.__str__()
