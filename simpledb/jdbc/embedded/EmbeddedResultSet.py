#
#  * The embedded implementation of ResultSet.
#  * @author Edward Sciore
#
from simpledb.jdbc.ResultSetAdapter import ResultSetAdapter
from simpledb.jdbc.SQLException import SQLException
from simpledb.jdbc.embedded.EmbeddedMetaData import EmbeddedMetaData


class EmbeddedResultSet(ResultSetAdapter):

    #
    #     * Creates a Scan object from the specified plan.
    #     * @param plan the query plan
    #     * @param conn the connection
    #     * @throws RemoteException
    #
    def __init__(self, plan, conn):
        super(EmbeddedResultSet, self).__init__()
        self.s = plan.open()
        self.sch = plan.schema()
        self.conn = conn

    #
    #     * Moves to the next record in the result set,
    #     * by moving to the next record in the saved scan.
    #
    def next(self):
        try:
            return self.s.next()
        except RuntimeError as e:
            self.conn.rollback()
            raise SQLException(e)

    #
    #     * Returns the integer value of the specified field,
    #     * by returning the corresponding value on the saved scan.
    #
    def getInt(self, fldname):
        try:
            fldname = fldname.lower()  # to ensure case-insensitivity
            return self.s.getInt(fldname)
        except RuntimeError as e:
            self.conn.rollback()
            raise SQLException(e)

    #
    #     * Returns the integer value of the specified field,
    #     * by returning the corresponding value on the saved scan.
    #
    def getString(self, fldname):
        try:
            fldname = fldname.lower()  # to ensure case-insensitivity
            return self.s.getString(fldname)
        except RuntimeError as e:
            self.conn.rollback()
            raise SQLException(e)

    #
    #     * Returns the result set's metadata,
    #     * by passing its schema into the EmbeddedMetaData constructor.
    #
    def getMetaData(self):
        return EmbeddedMetaData(self.sch)

    #
    #     * Closes the result set by closing its scan, and commits.
    #
    def close(self):
        self.s.close()
        self.conn.commit()
