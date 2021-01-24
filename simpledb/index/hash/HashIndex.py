#
#  * A static hash implementation of the Index interface.
#  * A fixed number of buckets is allocated (currently, 100),
#  * and each bucket is implemented as a file of index records.
#  * @author Edward Sciore
#
from simpledb.index.Index import Index
from simpledb.record.RID import RID
from simpledb.record.TableScan import TableScan


class HashIndex(Index):
    NUM_BUCKETS = 100

    #
    #    * Opens a hash index for the specified index.
    #    * @param idxname the name of the index
    #    * @param sch the schema of the index records
    #    * @param tx the calling transaction
    #
    def __init__(self, tx, idxname, layout):
        super(HashIndex, self).__init__()
        self.tx = tx
        self.idxname = idxname
        self.layout = layout

    #
    #    * Positions the index before the first index record
    #    * having the specified search key.
    #    * The method hashes the search key to determine the bucket,
    #    * and then opens a table scan on the file
    #    * corresponding to the bucket.
    #    * The table scan for the previous bucket (if any) is closed.
    #    * @see simpledb.index.Index#beforeFirst(simpledb.query.Constant)
    #
    def beforeFirst(self, searchkey):
        self.close()
        self.searchkey = searchkey
        bucket = searchkey.__hash__() % HashIndex.NUM_BUCKETS
        tblname = self.idxname + bucket
        self.ts = TableScan(self.tx, tblname, self.layout)

    #
    #    * Moves to the next record having the search key.
    #    * The method loops through the table scan for the bucket,
    #    * looking for a matching record, and returning false
    #    * if there are no more such records.
    #    * @see simpledb.index.Index#next()
    #
    def next(self):
        while self.ts.next():
            if self.ts.getVal("dataval") == self.searchkey:
                return True
        return False

    #
    #    * Retrieves the dataRID from the current record
    #    * in the table scan for the bucket.
    #    * @see simpledb.index.Index#getDataRid()
    #
    def getDataRid(self):
        blknum = self.ts.getInt("block")
        _id = self.ts.getInt("id")
        return RID(blknum, _id)

    #
    #    * Inserts a new record into the table scan for the bucket.
    #    * @see simpledb.index.Index#insert(simpledb.query.Constant, simpledb.record.RID)
    #
    def insert(self, val, rid):
        self.beforeFirst(val)
        self.ts.insert()
        self.ts.setInt("block", rid.blockNumber())
        self.ts.setInt("id", rid.slot())
        self.ts.setVal("dataval", val)

    #
    #    * Deletes the specified record from the table scan for
    #    * the bucket.  The method starts at the beginning of the
    #    * scan, and loops through the records until the
    #    * specified record is found.
    #    * @see simpledb.index.Index#delete(simpledb.query.Constant, simpledb.record.RID)
    #
    def delete(self, val, rid):
        self.beforeFirst(val)
        while self.next():
            if self.getDataRid() == rid:
                self.ts.delete()
                return

    #
    #    * Closes the index by closing the current table scan.
    #    * @see simpledb.index.Index#close()
    #
    def close(self):
        if self.ts is not None:
            self.ts.close()

    #
    #    * Returns the cost of searching an index file having the
    #    * specified number of blocks.
    #    * The method assumes that all buckets are about the
    #    * same size, and so the cost is simply the size of
    #    * the bucket.
    #    * @param numblocks the number of blocks of index records
    #    * @param rpb the number of records per block (not used here)
    #    * @return the cost of traversing the index
    #
    @staticmethod
    def searchCost(numblocks, rpb):
        return numblocks / HashIndex.NUM_BUCKETS
