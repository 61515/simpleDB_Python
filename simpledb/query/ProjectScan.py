
#
#  * The scan class corresponding to the <i>project</i> relational
#  * algebra operator.
#  * All methods except hasField delegate their work to the
#  * underlying scan.
#  * @author Edward Sciore
#
from simpledb.query.Scan import Scan


class ProjectScan(Scan):

    #
    #     * Create a project scan having the specified
    #     * underlying scan and field list.
    #     * @param s the underlying scan
    #     * @param fieldlist the list of field names
    #
    def __init__(self, s, fieldlist):
        super(ProjectScan, self).__init__()
        self.s = s
        self.fieldlist = fieldlist

    def beforeFirst(self):
        self.s.beforeFirst()

    def next(self):
        return self.s.next()

    def getInt(self, fldname):
        if self.hasField(fldname):
            return self.s.getInt(fldname)
        else:
            raise RuntimeError("field " + fldname + " not found.")

    def getString(self, fldname):
        if self.hasField(fldname):
            return self.s.getString(fldname)
        else:
            raise RuntimeError("field " + fldname + " not found.")

    def getVal(self, fldname):
        if self.hasField(fldname):
            return self.s.getVal(fldname)
        else:
            raise RuntimeError("field " + fldname + " not found.")

    def hasField(self, fldname):
        return self.fieldlist.count(fldname) > 0

    def close(self):
        self.s.close()
