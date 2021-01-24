#
#  * A term is a comparison between two expressions.
#  * @author Edward Sciore
#  *
#
from simpledb.util.Integer import Integer


class Term(object):

    #
    #     * Create a new term that compares two expressions
    #     * for equality.
    #     * @param lhs  the LHS expression
    #     * @param rhs  the RHS expression
    #
    def __init__(self, lhs, rhs):
        self.lhs = lhs
        self.rhs = rhs

    #
    #     * Return true if both of the term's expressions
    #     * evaluate to the same constant,
    #     * with respect to the specified scan.
    #     * @param s the scan
    #     * @return true if both expressions have the same value in the scan
    #
    def isSatisfied(self, s):
        lhsval = self.lhs.evaluate(s)
        rhsval = self.rhs.evaluate(s)
        return rhsval == lhsval

    #
    #     * Calculate the extent to which selecting on the term reduces
    #     * the number of records output by a query.
    #     * For example if the reduction factor is 2, then the
    #     * term cuts the size of the output in half.
    #     * @param p the query's plan
    #     * @return the integer reduction factor.
    #
    def reductionFactor(self, p):
        if self.lhs.isFieldName() and self.rhs.isFieldName():
            lhsName = self.lhs.asFieldName()
            rhsName = self.rhs.asFieldName()
            return max(p.distinctValues(lhsName), p.distinctValues(rhsName))
        if self.lhs.isFieldName():
            lhsName = self.lhs.asFieldName()
            return p.distinctValues(lhsName)
        if self.rhs.isFieldName():
            rhsName = self.rhs.asFieldName()
            return p.distinctValues(rhsName)
        #  otherwise, the term equates constants
        if self.lhs.asConstant() == self.rhs.asConstant():
            return 1
        else:
            return Integer.MAX_VALUE

    #
    #     * Determine if this term is of the form "F=c"
    #     * where F is the specified field and c is some constant.
    #     * If so, the method returns that constant.
    #     * If not, the method returns null.
    #     * @param fldname the name of the field
    #     * @return either the constant or null
    #
    def equatesWithConstant(self, fldname):
        if self.lhs.isFieldName() and self.lhs.asFieldName() == fldname and not self.rhs.isFieldName():
            return self.rhs.asConstant()
        elif self.rhs.isFieldName() and self.rhs.asFieldName() == fldname and not self.lhs.isFieldName():
            return self.lhs.asConstant()
        else:
            return None

    #
    #     * Determine if this term is of the form "F1=F2"
    #     * where F1 is the specified field and F2 is another field.
    #     * If so, the method returns the name of that field.
    #     * If not, the method returns null.
    #     * @param fldname the name of the field
    #     * @return either the name of the other field, or null
    #
    def equatesWithField(self, fldname):
        if self.lhs.isFieldName() and self.lhs.asFieldName() == fldname and self.rhs.isFieldName():
            return self.rhs.asFieldName()
        elif self.rhs.isFieldName() and self.rhs.asFieldName() == fldname and self.lhs.isFieldName():
            return self.lhs.asFieldName()
        else:
            return None

    #
    #     * Return true if both of the term's expressions
    #     * apply to the specified schema.
    #     * @param sch the schema
    #     * @return true if both expressions apply to the schema
    #
    def appliesTo(self, sch):
        return self.lhs.appliesTo(sch) and self.rhs.appliesTo(sch)

    def __str__(self):
        return self.lhs.__str__() + "=" + self.rhs.__str__()
