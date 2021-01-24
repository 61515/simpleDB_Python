class LockAbortException(RuntimeError):
    def __init__(self):
        super(LockAbortException, self).__init__()
