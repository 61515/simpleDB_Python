class BlockId:
    def __init__(self, filename, blknum):
        self.filename = filename
        self.blknum = blknum

    def fileName(self):
        return self.filename

    def number(self):
        return self.blknum

    def __eq__(self, other):
        return self.filename == other.filename and self.blknum == other.blknum

    def __str__(self):
        return "[file " + str(self.filename) + ", block " + str(self.blknum) + "]"

    def __hash__(self):
        return self.__str__().__hash__()
