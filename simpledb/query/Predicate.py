#
#  * A predicate is a Boolean combination of terms.
#  * @author Edward Sciore
#  *
#
from simpledb.record.Schema import Schema


class Predicate(object):

    #
    #     * Create an empty predicate, corresponding to "true".
    #
    def __init__(self, *args):
        self.terms = []
        #
        #     * Create a predicate containing a single term.
        #     * @param t the term
        #
        if len(args) == 1:
            t = args[0]
            self.terms.append(t)

    #
    #     * Modifies the predicate to be the conjunction of
    #     * itself and the specified predicate.
    #     * @param pred the other predicate
    #
    def conjoinWith(self, pred):
        for t in pred.terms:
            self.terms.append(t)

    #
    #     * Returns true if the predicate evaluates to true
    #     * with respect to the specified scan.
    #     * @param s the scan
    #     * @return true if the predicate is true in the scan
    #
    def isSatisfied(self, s):

        for t in self.terms:
            if not t.isSatisfied(s):
                return False
        return True

    #
    #     * Calculate the extent to which selecting on the predicate
    #     * reduces the number of records output by a query.
    #     * For example if the reduction factor is 2, then the
    #     * predicate cuts the size of the output in half.
    #     * @param p the query's plan
    #     * @return the integer reduction factor.
    #
    def reductionFactor(self, p):
        factor = 1
        for t in self.terms:
            factor *= t.reductionFactor(p)
        return factor

    #
    #     * Return the subpredicate that applies to the specified schema.
    #     * @param sch the schema
    #     * @return the subpredicate applying to the schema
    #
    def selectSubPred(self, sch):
        result = Predicate()
        for t in self.terms:
            if t.appliesTo(sch):
                result.terms.append(t)
        if len(result.terms) == 0:
            return None
        else:
            return result

    #
    #     * Return the subpredicate consisting of terms that apply
    #     * to the union of the two specified schemas,
    #     * but not to either schema separately.
    #     * @param sch1 the first schema
    #     * @param sch2 the second schema
    #     * @return the subpredicate whose terms apply to the union of the two schemas but not either schema separately.
    #
    def joinSubPred(self, sch1, sch2):
        result = Predicate()
        newsch = Schema()
        newsch.addAll(sch1)
        newsch.addAll(sch2)
        for t in self.terms:
            if not t.appliesTo(sch1) and not t.appliesTo(sch2) and t.appliesTo(newsch):
                result.terms.append(t)
        if len(result.terms) == 0:
            return None
        else:
            return result

    #
    #     * Determine if there is a term of the form "F=c"
    #     * where F is the specified field and c is some constant.
    #     * If so, the method returns that constant.
    #     * If not, the method returns null.
    #     * @param fldname the name of the field
    #     * @return either the constant or null
    #
    def equatesWithConstant(self, fldname):
        for t in self.terms:
            c = t.equatesWithConstant(fldname)
            if c is not None:
                return c
        return None

    #
    #     * Determine if there is a term of the form "F1=F2"
    #     * where F1 is the specified field and F2 is another field.
    #     * If so, the method returns the name of that field.
    #     * If not, the method returns null.
    #     * @param fldname the name of the field
    #     * @return the name of the other field, or null
    #
    def equatesWithField(self, fldname):
        for t in self.terms:
            s = t.equatesWithField(fldname)
            if s is not None:
                return s
        return None

    def __str__(self):
        if len(self.terms) == 0:
            return ""
        result = self.terms[0].__str__()
        for i in range(1, len(self.terms)):
            result += " and " + self.terms[i].__str__()
        return result
