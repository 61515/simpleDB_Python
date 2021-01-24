#
#  * Description of the structure of a record.
#  * It contains the name, type, length and offset of
#  * each field of the table.
#  * @author Edward Sciore
#  *
#
from simpledb.file.Page import Page
from simpledb.util.Integer import Integer
from simpledb.util.Types import INTEGER


class Layout(object):

    def __init__(self, *args):
        if len(args) == 1:
            schema = args[0]

            #
            #     * This constructor creates a Layout object from a schema.
            #     * This constructor is used when a table
            #     * is created. It determines the physical offset of
            #     * each field within the record.
            #     * @param tblname the name of the table
            #     * @param schema the schema of the table's records
            #
            self._schema = schema
            self.offsets = {}
            pos = Integer.BYTES  # leave space for the empty/inuse flag
            for fldname in schema.fields():
                self.offsets[fldname] = pos
                pos += self.lengthInBytes(fldname)
            self.slotsize = pos
        elif len(args) == 3:
            schema = args[0]
            offsets = args[1]
            slotsize = args[2]
            #
            #     * Create a Layout object from the specified metadata.
            #     * This constructor is used when the metadata
            #     * is retrieved from the catalog.
            #     * @param tblname the name of the table
            #     * @param schema the schema of the table's records
            #     * @param offsets the already-calculated offsets of the fields within a record
            #     * @param recordlen the already-calculated length of each record
            #
            self._schema = schema
            self.offsets = offsets
            self.slotsize = slotsize

    #
    #     * Return the schema of the table's records
    #     * @return the table's record schema
    #
    def schema(self):
        return self._schema

    #
    #     * Return the offset of a specified field within a record
    #     * @param fldname the name of the field
    #     * @return the offset of that field within a record
    #
    def offset(self, fldname):
        return self.offsets.get(fldname)

    #
    #     * Return the size of a slot, in bytes.
    #     * @return the size of a slot
    #
    def slotSize(self):
        return self.slotsize

    def lengthInBytes(self, fldname):
        fldtype = self._schema.type(fldname)
        if fldtype == INTEGER:
            return Integer.BYTES
        else:
            #  fldtype == VARCHAR
            return Page.maxLength(self._schema.length(fldname))
