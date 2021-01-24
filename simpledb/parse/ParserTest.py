from simpledb.parse.BadSyntaxException import BadSyntaxException
from simpledb.parse.Parser import Parser


class ParserTest(object):
    @classmethod
    def main(cls, args):
        print("Enter an SQL statement: ")
        while True:
            s = sys.stdin.readline()
            if not s:
                break

            p = Parser(s)
            try:
                if s.startswith("select"):
                    p.query()
                else:
                    p.updateCmd()
                print("yes")
            except BadSyntaxException as ex:
                print("no")
            print("Enter an SQL statement: ")


if __name__ == '__main__':
    import sys
    ParserTest.main(sys.argv)
