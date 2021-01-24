#
#  * A runtime exception indicating that the submitted query
#  * has incorrect syntax.
#  * @author Edward Sciore
#


class BadSyntaxException(RuntimeError):
    def __init__(self):
        super(BadSyntaxException, self).__init__()
