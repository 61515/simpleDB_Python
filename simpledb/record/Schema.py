#
#  * The record schema of a table.
#  * A schema contains the name and type of
#  * each field of the table, as well as the length
#  * of each varchar field.
#  * @author Edward Sciore
#  *
#
from simpledb.util.Types import INTEGER, VARCHAR


class Schema(object):

    def __init__(self):
        self._fields = []
        self.info = {}

    #     * Add a field to the schema having a specified
    #     * name, type, and length.
    #     * If the field type is "integer", then the length
    #     * value is irrelevant.
    #     * @param fldname the name of the field
    #     * @param type the type of the field, according to the constants in simpledb.sql.types
    #     * @param length the conceptual length of a string field.
    #
    def addField(self, fldname, _type, length):
        self._fields.append(fldname)
        self.info[fldname] = FieldInfo(_type, length)

    #
    #     * Add an integer field to the schema.
    #     * @param fldname the name of the field
    #
    def addIntField(self, fldname):
        self.addField(fldname, INTEGER, 0)

    #
    #     * Add a string field to the schema.
    #     * The length is the conceptual length of the field.
    #     * For example, if the field is defined as varchar(8),
    #     * then its length is 8.
    #     * @param fldname the name of the field
    #     * @param length the number of chars in the varchar definition
    #
    def addStringField(self, fldname, length):
        self.addField(fldname, VARCHAR, length)

    #
    #     * Add a field to the schema having the same
    #     * type and length as the corresponding field
    #     * in another schema.
    #     * @param fldname the name of the field
    #     * @param sch the other schema
    #
    def add(self, fldname, sch):
        _type = sch.type(fldname)
        length = sch.length(fldname)
        self.addField(fldname, _type, length)

    #
    #     * Add all of the fields in the specified schema
    #     * to the current schema.
    #     * @param sch the other schema
    #
    def addAll(self, sch):
        for fldname in sch.fields():
            self.add(fldname, sch)

    #
    #     * Return a collection containing the name of
    #     * each field in the schema.
    #     * @return the collection of the schema's field names
    #
    def fields(self):
        return self._fields

    #
    #     * Return true if the specified field
    #     * is in the schema
    #     * @param fldname the name of the field
    #     * @return true if the field is in the schema
    #
    def hasField(self, fldname):
        return self._fields.count(fldname) > 0

    #
    #     * Return the type of the specified field, using the
    #     * constants in {@link java.sql.Types}.
    #     * @param fldname the name of the field
    #     * @return the integer type of the field
    #
    def type(self, fldname):
        return self.info.get(fldname)._type

    #
    #     * Return the conceptual length of the specified field.
    #     * If the field is not a string field, then
    #     * the return value is undefined.
    #     * @param fldname the name of the field
    #     * @return the conceptual length of the field
    #
    def length(self, fldname):
        return self.info.get(fldname).length


class FieldInfo(object):

    def __init__(self, _type, length):
        self._type = _type
        self.length = length
