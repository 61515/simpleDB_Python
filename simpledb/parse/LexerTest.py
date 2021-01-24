#  Will successfully read in lines of text denoting an
#  SQL expression of the form "id = c" or "c = id".
from simpledb.parse.Lexer import Lexer


class LexerTest(object):
    @classmethod
    def main(cls, args):
        while True:
            s = sys.stdin.readline()
            if not s:
                break
            lex = Lexer(s)

            if lex.matchId():
                x = lex.eatId()
                lex.eatDelim('=')
                y = lex.eatIntConstant()
            else:
                y = lex.eatIntConstant()
                lex.eatDelim('=')
                x = lex.eatId()
            print(x.__str__() + " equals " + y.__str__())


if __name__ == '__main__':
    import sys
    LexerTest.main(sys.argv)

