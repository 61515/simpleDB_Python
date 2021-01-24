#
#  * The embedded implementation of Connection.
#  * @author Edward Sciore
#
from simpledb.jdbc.ConnectionAdapter import ConnectionAdapter
from simpledb.jdbc.SQLException import SQLException
from simpledb.jdbc.embedded.EmbeddedStatement import EmbeddedStatement


class EmbeddedConnection(ConnectionAdapter):

    #
    #     * Creates a connection
    #     * and begins a new transaction for it.
    #     * @throws RemoteException
    #
    def __init__(self, db):
        super(EmbeddedConnection, self).__init__()
        self.db = db
        self.currentTx = db.newTx()
        self.planner = db.planner()

    #
    #     * Creates a new Statement for this connection.
    #
    def createStatement(self):
        try:
            return EmbeddedStatement(self, self.planner)
        except Exception:
            raise SQLException()

    #
    #     * Closes the connection by committing the current transaction.
    #
    def close(self):
        try:
            self.currentTx.commit()
        except Exception:
            raise SQLException()

    #
    #     * Commits the current transaction and begins a new one.
    #
    def commit(self):
        try:
            self.currentTx.commit()
            self.currentTx = self.db.newTx()
        except Exception:
            raise SQLException()

    #
    #     * Rolls back the current transaction and begins a new one.
    #
    def rollback(self):
        try:
            self.currentTx.rollback()
            self.currentTx = self.db.newTx()
        except Exception:
            raise SQLException()

    #
    #     * Returns the transaction currently associated with
    #     * this connection. Not public. Called by other JDBC classes.
    #     * @return the transaction associated with this connection
    #
    def getTransaction(self):
        return self.currentTx
