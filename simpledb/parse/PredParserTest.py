from simpledb.parse.BadSyntaxException import BadSyntaxException
from simpledb.parse.PredParser import PredParser


class PredParserTest(object):
    @classmethod
    def main(cls, args):
        print("Enter an SQL predicate: ")
        while True:
            s = sys.stdin.readline()
            if not s:
                break
            p = PredParser(s)
            try:
                p.predicate()
                print("yes")
            except BadSyntaxException as ex:
                print("no")
            print("Enter an SQL predicate: ")


if __name__ == '__main__':
    import sys
    PredParserTest.main(sys.argv)
