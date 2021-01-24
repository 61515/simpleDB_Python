
#
#  * The embedded implementation of Statement.
#  * @author Edward Sciore
#
from simpledb.jdbc.SQLException import SQLException
from simpledb.jdbc.StatementAdapter import StatementAdapter
from simpledb.jdbc.embedded.EmbeddedResultSet import EmbeddedResultSet


class EmbeddedStatement(StatementAdapter):

    def __init__(self, conn, planner):
        super(EmbeddedStatement, self).__init__()
        self.conn = conn
        self.planner = planner

    #
    #     * Executes the specified SQL query string.
    #     * Calls the query planner to create a plan for the query,
    #     * and sends the plan to the ResultSet constructor for processing.
    #     * Rolls back and throws an SQLException if it cannot create the plan.
    #
    def executeQuery(self, qry):
        try:
            tx = self.conn.getTransaction()
            pln = self.planner.createQueryPlan(qry, tx)
            return EmbeddedResultSet(pln, self.conn)
        except RuntimeError as e:
            self.conn.rollback()
            raise SQLException(e)

    #
    #     * Executes the specified SQL update command by sending
    #     * the command to the update planner and then committing.
    #     * Rolls back and throws an SQLException on an error.
    #
    def executeUpdate(self, cmd):
        try:
            tx = self.conn.getTransaction()
            result = self.planner.executeUpdate(cmd, tx)
            self.conn.commit()
            return result
        except RuntimeError as e:
            self.conn.rollback()
            raise SQLException(e)

    def close(self):
        """ method close """
