#
#  * The interface corresponding to SQL expressions.
#  * @author Edward Sciore
#  *
#
from simpledb.query.Constant import Constant


class Expression(object):

    def __init__(self, val):
        if isinstance(val, str):
            self.fldname = val
            self.val = None
        elif isinstance(val, Constant):
            self.fldname = None
            self.val = val

    #
    #     * Evaluate the expression with respect to the
    #     * current record of the specified scan.
    #     * @param s the scan
    #     * @return the value of the expression, as a Constant
    #
    def evaluate(self, s):
        return self.val if (self.val is not None) else s.getVal(self.fldname)

    #
    #     * Return true if the expression is a field reference.
    #     * @return true if the expression denotes a field
    #
    def isFieldName(self):
        return self.fldname is not None

    #
    #     * Return the constant corresponding to a constant expression,
    #     * or null if the expression does not
    #     * denote a constant.
    #     * @return the expression as a constant
    #
    def asConstant(self):
        return self.val

    #
    #     * Return the field name corresponding to a constant expression,
    #     * or null if the expression does not
    #     * denote a field.
    #     * @return the expression as a field name
    #
    def asFieldName(self):
        return self.fldname

    #
    #     * Determine if all of the fields mentioned in this expression
    #     * are contained in the specified schema.
    #     * @param sch the schema
    #     * @return true if all fields in the expression are in the schema
    #
    def appliesTo(self, sch):
        return True if (self.val is not None) else sch.hasField(self.fldname)

    def __str__(self):
        return self.val.__str__() if (self.val is not None) else self.fldname
