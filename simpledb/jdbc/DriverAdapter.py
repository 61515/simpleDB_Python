
#
#  * This class implements all of the methods of the Driver interface,
#  * by throwing an exception for each one.
#  * Subclasses (such as SimpleDriver) can override those methods that
#  * it want to implement.
#  * @author Edward Sciore
#
from simpledb.jdbc.SQLException import SQLException


class DriverAdapter:
    def acceptsURL(self, url):
        raise SQLException("operation not implemented")

    def connect(self, url, info):
        raise SQLException("operation not implemented")

    def getMajorVersion(self):
        return 0

    def getMinorVersion(self):
        return 0

    def getPropertyInfo(self, url, info):
        return None

    def jdbcCompliant(self):
        return False

    def getParentLogger(self):
        """ method getParentLogger """
        # raise SQLFeatureNotSupportedException("operation not implemented")
