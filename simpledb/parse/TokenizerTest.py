from simpledb.util.StreamTokenizer import StreamTokenizer


class TokenizerTest(object):
    keywords = ["select", "from", "where", "and", "insert", "into", "values", "delete", "update", "set",
                "create", "table", "int", "varchar", "view", "as", "index", "on"]

    @classmethod
    def main(cls, args):
        s = TokenizerTest.getStringFromUser()
        tok = StreamTokenizer(s)
        tok.ordinaryChar('.')
        tok.lowerCaseMode(True)  # ids and keywords are converted to lower case
        while tok.nextToken() != StreamTokenizer.TT_EOF:
            TokenizerTest.printCurrentToken(tok)

    @staticmethod
    def getStringFromUser():
        print("Enter tokens:")
        # select a from x,z where b = 3
        s = sys.stdin.readline()
        return s

    @staticmethod
    def printCurrentToken(tok):
        if tok.ttype == StreamTokenizer.TT_NUMBER:
            print("IntConstant " + str(tok.nval))
        elif tok.ttype == StreamTokenizer.TT_WORD:
            word = tok.sval
            if TokenizerTest.keywords.count(word) > 0:
                print("Keyword " + word)
            else:
                print("Id " + word)
        elif tok.ttype == '\'':
            print("StringConstant " + tok.sval)
        else:
            print("Delimiter " + tok.ttype)


if __name__ == '__main__':
    import sys
    TokenizerTest.main(sys.argv)
