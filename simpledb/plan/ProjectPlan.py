
#  The Plan class corresponding to the <i>project</i>
#  * relational algebra operator.
#  * @author Edward Sciore
#
from simpledb.plan.Plan import Plan
from simpledb.query.ProjectScan import ProjectScan
from simpledb.record.Schema import Schema


class ProjectPlan(Plan):

    #
    #     * Creates a new project node in the query tree,
    #     * having the specified subquery and field list.
    #     * @param p the subquery
    #     * @param fieldlist the list of fields
    #
    def __init__(self, p, fieldlist):
        super(ProjectPlan, self).__init__()
        self.p = p
        self._schema = Schema()
        for fldname in fieldlist:
            self._schema.add(fldname, p.schema())

    #
    #     * Creates a project scan for this query.
    #     * @see Plan#open()
    #
    def open(self):
        s = self.p.open()
        return ProjectScan(s, self._schema.fields())

    #
    #     * Estimates the number of block accesses in the projection,
    #     * which is the same as in the underlying query.
    #     * @see Plan#blocksAccessed()
    #
    def blocksAccessed(self):
        return self.p.blocksAccessed()

    #
    #     * Estimates the number of output records in the projection,
    #     * which is the same as in the underlying query.
    #     * @see Plan#recordsOutput()
    #
    def recordsOutput(self):
        return self.p.recordsOutput()

    #
    #     * Estimates the number of distinct field values
    #     * in the projection,
    #     * which is the same as in the underlying query.
    #     * @see Plan#distinctValues(String)
    #
    def distinctValues(self, fldname):
        return self.p.distinctValues(fldname)

    #
    #     * Returns the schema of the projection,
    #     * which is taken from the field list.
    #     * @see Plan#schema()
    #
    def schema(self):
        return self._schema
