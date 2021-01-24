from simpledb.parse.Lexer import Lexer


class PredParser(object):

    def __init__(self, s):
        self.lex = Lexer(s)

    def field(self):
        return self.lex.eatId()

    def constant(self):
        if self.lex.matchStringConstant():
            self.lex.eatStringConstant()
        else:
            self.lex.eatIntConstant()

    def expression(self):
        if self.lex.matchId():
            self.field()
        else:
            self.constant()

    def term(self):
        self.expression()
        self.lex.eatDelim('=')
        self.expression()

    def predicate(self):
        self.term()
        if self.lex.matchKeyword("and"):
            self.lex.eatKeyword("and")
            self.predicate()
