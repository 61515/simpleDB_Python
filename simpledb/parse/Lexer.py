#
#  * The lexical analyzer.
#  * @author Edward Sciore
#
from simpledb.parse.BadSyntaxException import BadSyntaxException
from simpledb.util.StreamTokenizer import StreamTokenizer


class Lexer(object):

    #
    #     * Creates a new lexical analyzer for SQL statement s.
    #     * @param s the SQL statement
    #
    def __init__(self, s):
        self.keywords = []
        self.initKeywords()
        self.tok = StreamTokenizer(s)
        self.tok.ordinaryChar('.')  # disallow "." in identifiers
        self.tok.wordChars('_', '_')  # allow "_" in identifiers
        self.tok.lowerCaseMode(True)  # ids and keywords are converted
        self.nextToken()

    # Methods to check the status of the current token
    #
    #     * Returns true if the current token is
    #     * the specified delimiter character.
    #     * @param d a character denoting the delimiter
    #     * @return true if the delimiter is the current token
    #
    def matchDelim(self, d):
        return d == self.tok.ttype

    #
    #     * Returns true if the current token is an integer.
    #     * @return true if the current token is an integer
    #
    def matchIntConstant(self):
        return self.tok.ttype == StreamTokenizer.TT_NUMBER

    #
    #     * Returns true if the current token is a string.
    #     * @return true if the current token is a string
    #
    def matchStringConstant(self):
        return '\'' == self.tok.ttype

    #
    #     * Returns true if the current token is the specified keyword.
    #     * @param w the keyword string
    #     * @return true if that keyword is the current token
    #
    def matchKeyword(self, w):
        return self.tok.ttype == StreamTokenizer.TT_WORD and self.tok.sval == w

    #
    #     * Returns true if the current token is a legal identifier.
    #     * @return true if the current token is an identifier
    #
    def matchId(self):
        return self.tok.ttype == StreamTokenizer.TT_WORD and not (self.keywords.count(self.tok.sval) > 0)

    # Methods to "eat" the current token
    #
    #     * Throws an exception if the current token is not the
    #     * specified delimiter.
    #     * Otherwise, moves to the next token.
    #     * @param d a character denoting the delimiter
    #
    def eatDelim(self, d):
        if not self.matchDelim(d):
            raise BadSyntaxException()
        self.nextToken()

    #
    #     * Throws an exception if the current token is not
    #     * an integer.
    #     * Otherwise, returns that integer and moves to the next token.
    #     * @return the integer value of the current token
    #
    def eatIntConstant(self):
        if not self.matchIntConstant():
            raise BadSyntaxException()
        i = self.tok.nval
        self.nextToken()
        return i

    #
    #     * Throws an exception if the current token is not
    #     * a string.
    #     * Otherwise, returns that string and moves to the next token.
    #     * @return the string value of the current token
    #
    def eatStringConstant(self):
        if not self.matchStringConstant():
            raise BadSyntaxException()
        s = self.tok.sval
        # constants are not converted to lower case
        self.nextToken()
        return s

    #
    #     * Throws an exception if the current token is not the
    #     * specified keyword.
    #     * Otherwise, moves to the next token.
    #     * @param w the keyword string
    #
    def eatKeyword(self, w):
        if not self.matchKeyword(w):
            raise BadSyntaxException()
        self.nextToken()

    #
    #     * Throws an exception if the current token is not
    #     * an identifier.
    #     * Otherwise, returns the identifier string
    #     * and moves to the next token.
    #     * @return the string value of the current token
    #
    def eatId(self):
        if not self.matchId():
            raise BadSyntaxException()
        s = self.tok.sval
        self.nextToken()
        return s

    def nextToken(self):
        try:
            self.tok.nextToken()
        except IOError as e:
            raise BadSyntaxException()

    def initKeywords(self):
        self.keywords = ["select", "from", "where", "and", "insert", "into", "values", "delete", "update", "set",
                         "create", "table", "int", "varchar", "view", "as", "index", "on"]
