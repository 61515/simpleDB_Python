from simpledb.parse.BadSyntaxException import BadSyntaxException
from simpledb.parse.Parser import Parser


class ParserTestActions(object):
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
                    result = p.query().__str__()
                else:
                    result = p.updateCmd().__class__.__str__()
                print("Your statement is: " + result)
            except BadSyntaxException as ex:
                print("Your statement is illegal")
            print("Enter an SQL statement: ")


if __name__ == '__main__':
    import sys
    ParserTestActions.main(sys.argv)
