#
#  * An object that holds the contents of a B-tree leaf block.
#  * @author Edward Sciore
#
from simpledb.file.BlockId import BlockId
from simpledb.index.btree.BTPage import BTPage
from simpledb.index.btree.DirEntry import DirEntry


class BTreeLeaf(object):

    #
    #     * Opens a buffer to hold the specified leaf block.
    #     * The buffer is positioned immediately before the first record
    #     * having the specified search key (if any).
    #     * @param blk a reference to the disk block
    #     * @param layout the metadata of the B-tree leaf file
    #     * @param searchkey the search key value
    #     * @param tx the calling transaction
    #
    def __init__(self, tx, blk, layout, searchkey):
        self.tx = tx
        self.layout = layout
        self.searchkey = searchkey
        self.contents = BTPage(tx, blk, layout)
        self.currentslot = self.contents.findSlotBefore(searchkey)
        self.filename = blk.fileName()

    #
    #     * Closes the leaf page.
    #
    def close(self):
        self.contents.close()

    #
    #     * Moves to the next leaf record having the
    #     * previously-specified search key.
    #     * Returns false if there is no more such records.
    #     * @return false if there are no more leaf records for the search key
    #
    def next(self):
        self.currentslot += 1
        if self.currentslot >= self.contents.getNumRecs():
            return self.tryOverflow()
        elif self.contents.getDataVal(self.currentslot) == self.searchkey:
            return True
        else:
            return self.tryOverflow()

    #
    #     * Returns the dataRID value of the current leaf record.
    #     * @return the dataRID of the current record
    #
    def getDataRid(self):
        return self.contents.getDataRid(self.currentslot)

    #
    #     * Deletes the leaf record having the specified dataRID
    #     * @param datarid the dataRId whose record is to be deleted
    #
    def delete(self, datarid):
        while self.next():
            if self.getDataRid() == datarid:
                self.contents.delete(self.currentslot)
                return

    #
    #     * Inserts a new leaf record having the specified dataRID
    #     * and the previously-specified search key.
    #     * If the record does not fit in the page, then
    #     * the page splits and the method returns the
    #     * directory entry for the new page;
    #     * otherwise, the method returns null.
    #     * If all of the records in the page have the same dataval,
    #     * then the block does not split; instead, all but one of the
    #     * records are placed into an overflow block.
    #     * @param datarid the dataRID value of the new record
    #     * @return the directory entry of the newly-split page, if one exists.
    #
    def insert(self, datarid):
        if self.contents.getFlag() >= 0 and self.contents.getDataVal(0).__cmp__(self.searchkey) > 0:
            firstval = self.contents.getDataVal(0)
            newblk = self.contents.split(0, self.contents.getFlag())
            self.currentslot = 0
            self.contents.setFlag(-1)
            self.contents.insertLeaf(self.currentslot, self.searchkey, datarid)
            return DirEntry(firstval, newblk.number())

        self.currentslot += 1
        self.contents.insertLeaf(self.currentslot, self.searchkey, datarid)
        if not self.contents.isFull():
            return None
        #  else page is full, so split it
        firstkey = self.contents.getDataVal(0)
        lastkey = self.contents.getDataVal(self.contents.getNumRecs() - 1)
        if lastkey == firstkey:
            #  create an overflow block to hold all but the first record
            newblk = self.contents.split(1, self.contents.getFlag())
            self.contents.setFlag(newblk.number())
            return None
        else:
            splitpos = self.contents.getNumRecs() / 2
            splitkey = self.contents.getDataVal(splitpos)
            if splitkey == firstkey:
                #  move right, looking for the next key
                while self.contents.getDataVal(splitpos) == splitkey:
                    splitpos += 1
                splitkey = self.contents.getDataVal(splitpos)
            else:
                #  move left, looking for first entry having that key
                while self.contents.getDataVal(splitpos - 1) == splitkey:
                    splitpos -= 1
            newblk = self.contents.split(splitpos, -1)
            return DirEntry(splitkey, newblk.number())

    def tryOverflow(self):
        firstkey = self.contents.getDataVal(0)
        flag = self.contents.getFlag()
        if not self.searchkey == firstkey or flag < 0:
            return False
        self.contents.close()
        nextblk = BlockId(self.filename, flag)
        self.contents = BTPage(self.tx, nextblk, self.layout)
        self.currentslot = 0
        return True
