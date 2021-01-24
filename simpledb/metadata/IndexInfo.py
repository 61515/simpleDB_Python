# in case we change to btree indexing
#
#  * The information about an index.
#  * This information is used by the query planner in order to
#  * estimate the costs of using the index,
#  * and to obtain the layout of the index records.
#  * Its methods are essentially the same as those of Plan.
#  * @author Edward Sciore
#
from simpledb.index.hash.HashIndex import HashIndex
from simpledb.record.Layout import Layout
from simpledb.record.Schema import Schema
from simpledb.util.Types import INTEGER


class IndexInfo(object):

    #
    #     * Create an IndexInfo object for the specified index.
    #     * @param idxname the name of the index
    #     * @param fldname the name of the indexed field
    #     * @param tx the calling transaction
    #     * @param tblSchema the schema of the table
    #     * @param si the statistics for the table
    #
    def __init__(self, idxname, fldname, tblSchema, tx, si):
        self.idxname = idxname
        self.fldname = fldname
        self.tx = tx
        self.tblSchema = tblSchema
        self.idxLayout = self.createIdxLayout()
        self.si = si

    #
    #     * Open the index described by this object.
    #     * @return the Index object associated with this information
    #
    def open(self):
        return HashIndex(self.tx, self.idxname, self.idxLayout)
        # return new BTreeIndex(tx, idxname, idxLayout);

    #
    #     * Estimate the number of block accesses required to
    #     * find all index records having a particular search key.
    #     * The method uses the table's metadata to estimate the
    #     * size of the index file and the number of index records
    #     * per block.
    #     * It then passes this information to the traversalCost
    #     * method of the appropriate index type,
    #     * which provides the estimate.
    #     * @return the number of block accesses required to traverse the index
    #
    def blocksAccessed(self):
        rpb = self.tx.blockSize() / self.idxLayout.slotSize()
        numblocks = self.si.recordsOutput() / rpb
        return HashIndex.searchCost(numblocks, rpb)
        # return BTreeIndex.searchCost(numblocks, rpb);

    #
    #     * Return the estimated number of records having a
    #     * search key.  This value is the same as doing a select
    #     * query; that is, it is the number of records in the table
    #     * divided by the number of distinct values of the indexed field.
    #     * @return the estimated number of records having a search key
    #
    def recordsOutput(self):
        return self.si.recordsOutput() / self.si.distinctValues(self.fldname)

    #
    #     * Return the distinct values for a specified field
    #     * in the underlying table, or 1 for the indexed field.
    #     * @param fname the specified field
    #
    def distinctValues(self, fname):
        return 1 if self.fldname == fname else self.si.distinctValues(self.fldname)

    #
    #     * Return the layout of the index records.
    #     * The schema consists of the dataRID (which is
    #     * represented as two integers, the block number and the
    #     * record ID) and the dataval (which is the indexed field).
    #     * Schema information about the indexed field is obtained
    #     * via the table's schema.
    #     * @return the layout of the index records
    #
    def createIdxLayout(self):
        sch = Schema()
        sch.addIntField("block")
        sch.addIntField("id")
        if self.tblSchema.type(self.fldname) == INTEGER:
            sch.addIntField("dataval")
        else:
            fldlen = self.tblSchema.length(self.fldname)
            sch.addStringField("dataval", fldlen)
        return Layout(sch)
