
#
#  * The object that executes SQL statements.
#  * @author Edward Sciore
#
from simpledb.parse.CreateIndexData import CreateIndexData
from simpledb.parse.CreateTableData import CreateTableData
from simpledb.parse.CreateViewData import CreateViewData
from simpledb.parse.DeleteData import DeleteData
from simpledb.parse.InsertData import InsertData
from simpledb.parse.ModifyData import ModifyData
from simpledb.parse.Parser import Parser


class Planner(object):

    def __init__(self, qplanner, uplanner):
        self.qplanner = qplanner
        self.uplanner = uplanner

    #
    #     * Creates a plan for an SQL select statement, using the supplied planner.
    #     * @param qry the SQL query string
    #     * @param tx the transaction
    #     * @return the scan corresponding to the query plan
    #
    def createQueryPlan(self, qry, tx):
        parser = Parser(qry)
        data = parser.query()
        self.verifyQuery(data)
        return self.qplanner.createPlan(data, tx)

    #
    #     * Executes an SQL insert, delete, modify, or
    #     * create statement.
    #     * The method dispatches to the appropriate method of the
    #     * supplied update planner,
    #     * depending on what the parser returns.
    #     * @param cmd the SQL update string
    #     * @param tx the transaction
    #     * @return an integer denoting the number of affected records
    #
    def executeUpdate(self, cmd, tx):
        parser = Parser(cmd)
        data = parser.updateCmd()
        self.verifyUpdate(data)
        if isinstance(data, InsertData):
            return self.uplanner.executeInsert(data, tx)
        elif isinstance(data, DeleteData):
            return self.uplanner.executeDelete(data, tx)
        elif isinstance(data, ModifyData):
            return self.uplanner.executeModify(data, tx)
        elif isinstance(data, CreateTableData):
            return self.uplanner.executeCreateTable(data, tx)
        elif isinstance(data, CreateViewData):
            return self.uplanner.executeCreateView(data, tx)
        elif isinstance(data, CreateIndexData):
            return self.uplanner.executeCreateIndex(data, tx)
        else:
            return 0

    #  SimpleDB does not verify queries, although it should.
    def verifyQuery(self, data):
        """ method verifyQuery """

    #  SimpleDB does not verify updates, although it should.
    def verifyUpdate(self, data):
        """ method verifyUpdate """
