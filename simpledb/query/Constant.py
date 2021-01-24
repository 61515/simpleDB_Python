class Constant:
    def __init__(self, *args):
        if isinstance(args[0], str):
            self.sval = args[0]
            self.ival = None
        elif isinstance(args[0], int) or isinstance(args[0], float):
            self.sval = None
            self.ival = args[0]

    def asInt(self):
        return self.ival

    def asString(self):
        return self.sval

    def __eq__(self, other):
        # c = Constant(other)
        c = other
        return self.ival == c.ival if (self.ival is not None) else self.sval == c.sval

    def __cmp__(self, other):
        return self.ival.compareTo(other.ival) if (self.ival is not None) else self.sval.compareTo(other.sval)

    def __hash__(self):
        return self.ival.hashCode() if (self.ival is not None) else self.sval.hashCode()

    def __str__(self):
        return self.ival.__str__() if (self.ival is not None) else self.sval.__str__()


if __name__ == '__main__':
    c = Constant(123)
    print(c)
