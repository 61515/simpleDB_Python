from simpledb.record.Layout import Layout
from simpledb.record.Schema import Schema


class LayoutTest(object):
    @classmethod
    def main(cls, args):
        sch = Schema()
        sch.addIntField("A")
        sch.addStringField("B", 9)
        layout = Layout(sch)
        for fldname in layout.schema().fields():
            offset = layout.offset(fldname)
            print(fldname + " has offset " + str(offset))


if __name__ == '__main__':
    import sys
    LayoutTest.main(sys.argv)
