
#
#  * The RMI server-side implementation of RemoteDriver.
#  * @author Edward Sciore
#
from simpledb.jdbc.DriverAdapter import DriverAdapter
from simpledb.jdbc.SQLException import SQLException
from simpledb.jdbc.embedded.EmbeddedConnection import EmbeddedConnection
from simpledb.server.SimpleDB import SimpleDB


class EmbeddedDriver(DriverAdapter):

    #
    #     * Creates a new RemoteConnectionImpl object and
    #     * returns it.
    #     * @see simpledb.jdbc.network.RemoteDriver#connect()
    #
    def connect(self, url, p):
        try:
            dbname = url.replace("jdbc:simpledb:", "")
            db = SimpleDB(dbname)
            return EmbeddedConnection(db)
        except Exception:
            raise SQLException()
