#  * A runtime exception indicating that the transaction
#  * needs to abort because a buffer request could not be satisfied.
#  * @author Edward Sciore
#


# @SuppressWarnings("serial")
class BufferAbortException(RuntimeError):
    def __init__(self):
        super().__init__()
