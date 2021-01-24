#
#  * A B-tree implementation of the Index interface.
#  * @author Edward Sciore
#
from simpledb.file.BlockId import BlockId
from simpledb.index.Index import Index
from simpledb.index.btree.BTPage import BTPage
from simpledb.index.btree.BTreeDir import BTreeDir
from simpledb.index.btree.BTreeLeaf import BTreeLeaf
from simpledb.query.Constant import Constant
from simpledb.record.Layout import Layout
from simpledb.record.Schema import Schema
from simpledb.util.Integer import Integer
from simpledb.util.Types import INTEGER
import math


class BTreeIndex(Index):

    #
    #     * Opens a B-tree index for the specified index.
    #     * The method determines the appropriate files
    #     * for the leaf and directory records,
    #     * creating them if they did not exist.
    #     * @param idxname the name of the index
    #     * @param leafsch the schema of the leaf index records
    #     * @param tx the calling transaction
    #
    def __init__(self, tx, idxname, leafLayout):
        super(BTreeIndex, self).__init__()
        self.tx = tx
        #  deal with the leaves
        self.leaftbl = idxname + "leaf"
        self.leafLayout = leafLayout
        if tx.size(self.leaftbl) == 0:
            blk = tx.append(self.leaftbl)
            node = BTPage(tx, blk, leafLayout)
            node.format(blk, -1)

        #  deal with the directory
        dirsch = Schema()
        dirsch.add("block", leafLayout.schema())
        dirsch.add("dataval", leafLayout.schema())
        dirtbl = idxname + "dir"
        self.dirLayout = Layout(dirsch)
        self.rootblk = BlockId(dirtbl, 0)
        if tx.size(dirtbl) == 0:
            #  create new root block
            tx.append(dirtbl)
            node = BTPage(tx, self.rootblk, self.dirLayout)
            node.format(self.rootblk, 0)
            #  insert initial directory entry
            fldtype = dirsch.type("dataval")
            minval = Constant(Integer.MIN_VALUE) if fldtype == INTEGER else Constant("")
            node.insertDir(0, minval, 0)
            node.close()

    #
    #     * Traverse the directory to find the leaf block corresponding
    #     * to the specified search key.
    #     * The method then opens a page for that leaf block, and
    #     * positions the page before the first record (if any)
    #     * having that search key.
    #     * The leaf page is kept open, for use by the methods next
    #     * and getDataRid.
    #     * @see simpledb.index.Index#beforeFirst(simpledb.query.Constant)
    #
    def beforeFirst(self, searchkey):
        self.close()
        root = BTreeDir(self.tx, self.rootblk, self.dirLayout)
        blknum = root.search(searchkey)
        root.close()
        leafblk = BlockId(self.leaftbl, blknum)
        self.leaf = BTreeLeaf(self.tx, leafblk, self.leafLayout, searchkey)

    #
    #     * Move to the next leaf record having the
    #     * previously-specified search key.
    #     * Returns false if there are no more such leaf records.
    #     * @see simpledb.index.Index#next()
    #
    def next(self):
        return self.leaf.next()

    #
    #     * Return the dataRID value from the current leaf record.
    #     * @see simpledb.index.Index#getDataRid()
    #
    def getDataRid(self):
        return self.leaf.getDataRid()

    #
    #     * Insert the specified record into the index.
    #     * The method first traverses the directory to find
    #     * the appropriate leaf page; then it inserts
    #     * the record into the leaf.
    #     * If the insertion causes the leaf to split, then
    #     * the method calls insert on the root,
    #     * passing it the directory entry of the new leaf page.
    #     * If the root node splits, then makeNewRoot is called.
    #     * @see simpledb.index.Index#insert(simpledb.query.Constant, simpledb.record.RID)
    #
    def insert(self, dataval, datarid):
        self.beforeFirst(dataval)
        e = self.leaf.insert(datarid)
        self.leaf.close()
        if e is None:
            return
        root = BTreeDir(self.tx, self.rootblk, self.dirLayout)
        e2 = root.insert(e)
        if e2 is not None:
            root.makeNewRoot(e2)
        root.close()

    #
    #     * Delete the specified index record.
    #     * The method first traverses the directory to find
    #     * the leaf page containing that record; then it
    #     * deletes the record from the page.
    #     * @see simpledb.index.Index#delete(simpledb.query.Constant, simpledb.record.RID)
    #
    def delete(self, dataval, datarid):
        self.beforeFirst(dataval)
        self.leaf.delete(datarid)
        self.leaf.close()

    #
    #     * Close the index by closing its open leaf page,
    #     * if necessary.
    #     * @see simpledb.index.Index#close()
    #
    def close(self):
        if self.leaf is not None:
            self.leaf.close()

    #
    #     * Estimate the number of block accesses
    #     * required to find all index records having
    #     * a particular search key.
    #     * @param numblocks the number of blocks in the B-tree directory
    #     * @param rpb the number of index entries per block
    #     * @return the estimated traversal cost
    #
    @staticmethod
    def searchCost(numblocks, rpb):
        return 1 + int(math.log(numblocks) / math.log(rpb))
