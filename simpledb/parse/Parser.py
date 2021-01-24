
#
#  * The SimpleDB parser.
#  * @author Edward Sciore
#
from simpledb.parse.CreateIndexData import CreateIndexData
from simpledb.parse.CreateTableData import CreateTableData
from simpledb.parse.CreateViewData import CreateViewData
from simpledb.parse.DeleteData import DeleteData
from simpledb.parse.InsertData import InsertData
from simpledb.parse.Lexer import Lexer
from simpledb.parse.ModifyData import ModifyData
from simpledb.parse.QueryData import QueryData
from simpledb.query.Constant import Constant
from simpledb.query.Expression import Expression
from simpledb.query.Predicate import Predicate
from simpledb.query.Term import Term
from simpledb.record.Schema import Schema


class Parser(object):

    def __init__(self, s):
        self.lex = Lexer(s)

    #  Methods for parsing predicates, terms, expressions, constants, and fields
    def field(self):
        return self.lex.eatId()

    def constant(self):
        if self.lex.matchStringConstant():
            return Constant(self.lex.eatStringConstant())
        else:
            return Constant(self.lex.eatIntConstant())

    def expression(self):
        if self.lex.matchId():
            return Expression(self.field())
        else:
            return Expression(self.constant())

    def term(self):
        lhs = self.expression()
        self.lex.eatDelim('=')
        rhs = self.expression()
        return Term(lhs, rhs)

    def predicate(self):
        pred = Predicate(self.term())
        if self.lex.matchKeyword("and"):
            self.lex.eatKeyword("and")
            pred.conjoinWith(self.predicate())
        return pred

    #  Methods for parsing queries
    def query(self):
        self.lex.eatKeyword("select")
        fields = self.selectList()
        self.lex.eatKeyword("from")
        tables = self.tableList()
        pred = Predicate()
        if self.lex.matchKeyword("where"):
            self.lex.eatKeyword("where")
            pred = self.predicate()
        return QueryData(fields, tables, pred)

    def selectList(self):
        L = [self.field()]
        if self.lex.matchDelim(','):
            self.lex.eatDelim(',')
            for item in self.selectList():
                L.append(item)
        return L

    def tableList(self):
        L = [self.lex.eatId()]
        if self.lex.matchDelim(','):
            self.lex.eatDelim(',')
            for item in self.tableList():
                L.append(item)
        return L

    #  Methods for parsing the various update commands
    def updateCmd(self):
        if self.lex.matchKeyword("insert"):
            return self.insert()
        elif self.lex.matchKeyword("delete"):
            return self.delete()
        elif self.lex.matchKeyword("update"):
            return self.modify()
        else:
            return self.create()

    def create(self):
        self.lex.eatKeyword("create")
        if self.lex.matchKeyword("table"):
            return self.createTable()
        elif self.lex.matchKeyword("view"):
            return self.createView()
        else:
            return self.createIndex()

    #  Method for parsing delete commands
    def delete(self):
        self.lex.eatKeyword("delete")
        self.lex.eatKeyword("from")
        tblname = self.lex.eatId()
        pred = Predicate()
        if self.lex.matchKeyword("where"):
            self.lex.eatKeyword("where")
            pred = self.predicate()
        return DeleteData(tblname, pred)

    #  Methods for parsing insert commands
    def insert(self):
        self.lex.eatKeyword("insert")
        self.lex.eatKeyword("into")
        tblname = self.lex.eatId()
        self.lex.eatDelim('(')
        flds = self.fieldList()
        self.lex.eatDelim(')')
        self.lex.eatKeyword("values")
        self.lex.eatDelim('(')
        vals = self.constList()
        self.lex.eatDelim(')')
        return InsertData(tblname, flds, vals)

    def fieldList(self):
        L = [self.field()]
        if self.lex.matchDelim(','):
            self.lex.eatDelim(',')
            for item in self.fieldList():
                L.append(item)
        return L

    def constList(self):
        L = [self.constant()]
        if self.lex.matchDelim(','):
            self.lex.eatDelim(',')
            for item in self.constList():
                L.append(item)
        return L

    #  Method for parsing modify commands
    def modify(self):
        self.lex.eatKeyword("update")
        tblname = self.lex.eatId()
        self.lex.eatKeyword("set")
        fldname = self.field()
        self.lex.eatDelim('=')
        newval = self.expression()
        pred = Predicate()
        if self.lex.matchKeyword("where"):
            self.lex.eatKeyword("where")
            pred = self.predicate()
        return ModifyData(tblname, fldname, newval, pred)

    #  Method for parsing create table commands
    def createTable(self):
        self.lex.eatKeyword("table")
        tblname = self.lex.eatId()
        self.lex.eatDelim('(')
        sch = self.fieldDefs()
        self.lex.eatDelim(')')
        return CreateTableData(tblname, sch)

    def fieldDefs(self):
        schema = self.fieldDef()
        if self.lex.matchDelim(','):
            self.lex.eatDelim(',')
            schema2 = self.fieldDefs()
            schema.addAll(schema2)
        return schema

    def fieldDef(self):
        fldname = self.field()
        return self.fieldType(fldname)

    def fieldType(self, fldname):
        schema = Schema()
        if self.lex.matchKeyword("int"):
            self.lex.eatKeyword("int")
            schema.addIntField(fldname)
        else:
            self.lex.eatKeyword("varchar")
            self.lex.eatDelim('(')
            strLen = self.lex.eatIntConstant()
            self.lex.eatDelim(')')
            schema.addStringField(fldname, strLen)
        return schema

    #  Method for parsing create view commands
    def createView(self):
        self.lex.eatKeyword("view")
        viewname = self.lex.eatId()
        self.lex.eatKeyword("as")
        qd = self.query()
        return CreateViewData(viewname, qd)

    #   Method for parsing create index commands
    def createIndex(self):
        self.lex.eatKeyword("index")
        idxname = self.lex.eatId()
        self.lex.eatKeyword("on")
        tblname = self.lex.eatId()
        self.lex.eatDelim('(')
        fldname = self.field()
        self.lex.eatDelim(')')
        return CreateIndexData(idxname, tblname, fldname)
