#
#  * A B-tree directory block.
#  * @author Edward Sciore
#
from simpledb.file.BlockId import BlockId
from simpledb.index.btree.BTPage import BTPage
from simpledb.index.btree.DirEntry import DirEntry


class BTreeDir(object):

    #
    #     * Creates an object to hold the contents of the specified
    #     * B-tree block.
    #     * @param blk a reference to the specified B-tree block
    #     * @param layout the metadata of the B-tree directory file
    #     * @param tx the calling transaction
    #
    def __init__(self, tx, blk, layout):
        self.tx = tx
        self.layout = layout
        self.contents = BTPage(tx, blk, layout)
        self.filename = blk.fileName()

    #
    #     * Closes the directory page.
    #
    def close(self):
        self.contents.close()

    #
    #     * Returns the block number of the B-tree leaf block
    #     * that contains the specified search key.
    #     * @param searchkey the search key value
    #     * @return the block number of the leaf block containing that search key
    #
    def search(self, searchkey):
        childblk = self.findChildBlock(searchkey)
        while self.contents.getFlag() > 0:
            self.contents.close()
            self.contents = BTPage(self.tx, childblk, self.layout)
            childblk = self.findChildBlock(searchkey)
        return childblk.number()

    #
    #     * Creates a new root block for the B-tree.
    #     * The new root will have two children:
    #     * the old root, and the specified block.
    #     * Since the root must always be in block 0 of the file,
    #     * the contents of the old root will get transferred to a new block.
    #     * @param e the directory entry to be added as a child of the new root
    #
    def makeNewRoot(self, e):
        firstval = self.contents.getDataVal(0)
        level = self.contents.getFlag()
        newblk = self.contents.split(0, level)  # ie, transfer all the records
        oldroot = DirEntry(firstval, newblk.number())
        self.insertEntry(oldroot)
        self.insertEntry(e)
        self.contents.setFlag(level + 1)

    #
    #     * Inserts a new directory entry into the B-tree block.
    #     * If the block is at level 0, then the entry is inserted there.
    #     * Otherwise, the entry is inserted into the appropriate
    #     * child node, and the return value is examined.
    #     * A non-null return value indicates that the child node
    #     * split, and so the returned entry is inserted into
    #     * this block.
    #     * If this block splits, then the method similarly returns
    #     * the entry information of the new block to its caller;
    #     * otherwise, the method returns null.
    #     * @param e the directory entry to be inserted
    #     * @return the directory entry of the newly-split block, if one exists; otherwise, null
    #
    def insert(self, e):
        if self.contents.getFlag() == 0:
            return self.insertEntry(e)
        childblk = self.findChildBlock(e.dataVal())
        child = BTreeDir(self.tx, childblk, self.layout)
        myentry = child.insert(e)
        child.close()
        return self.insertEntry(myentry) if (myentry is not None) else None

    def insertEntry(self, e):
        newslot = 1 + self.contents.findSlotBefore(e.dataVal())
        self.contents.insertDir(newslot, e.dataVal(), e.blockNumber())
        if not self.contents.isFull():
            return None
        #  else page is full, so split it
        level = self.contents.getFlag()
        splitpos = self.contents.getNumRecs() / 2
        splitval = self.contents.getDataVal(splitpos)
        newblk = self.contents.split(splitpos, level)
        return DirEntry(splitval, newblk.number())

    def findChildBlock(self, searchkey):
        slot = self.contents.findSlotBefore(searchkey)
        if self.contents.getDataVal(slot + 1) == searchkey:
            slot += 1
        blknum = self.contents.getChildNum(slot)
        return BlockId(self.filename, blknum)
