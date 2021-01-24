class StreamTokenizer:
    TT_EOF = -1
    TT_WORD = -3
    TT_NUMBER = -2
    TT_NOTHING = -4

    def __init__(self, s):
        self.nval = None
        self.sval = None
        self.ttype = StreamTokenizer.TT_NOTHING
        self.input = s
        self.pos = 0
        self.end = len(s)
        self.lowercasemode = False

    def ordinaryChar(self, ch):
        """ method ordinaryChar """

    def wordChars(self, ch1, ch2):
        """ method wordChars """

    def lowerCaseMode(self, mode):
        """ method lowerCaseMode """
        self.lowercasemode = True

    def nextToken(self):
        list_delim = [",", "=", '(', ')']
        list_str = ["\'", "\""]
        list_trim = ['\n', '\t', ' ']
        # 判断当前是 id,delim,num,str
        while True:
            ch = self.getNow()
            if ch in list_trim:
                self.read()
                continue
            else:
                break

        if ch == StreamTokenizer.TT_EOF:
            return StreamTokenizer.TT_EOF

        # delim
        if ch in list_delim:
            self.pos += 1
            self.ttype = ch
            return ch

        # num
        elif '0' <= ch <= '9':
            _str = "" + ch
            while self.read():
                ch = self.getNow()
                if ch in list_trim or ch in list_delim:
                    break
                _str += ch

            try:
                self.sval = _str
                try:
                    self.nval = int(_str)
                except Exception:
                    self.nval = float(_str)
                self.ttype = StreamTokenizer.TT_NUMBER
                return self.ttype
            except Exception:
                raise IOError

        # str
        elif ch in list_str:
            _str = ""
            while self.read():
                ch2 = self.getNow()
                if ch2 == ch:
                    break
                _str += ch2
            else:
                raise IOError

            self.pos += 1
            self.sval = _str
            self.nval = None
            self.ttype = '\''
            return self.ttype
        # word
        else:
            _str = "" + str(ch)
            while self.read():
                ch = self.getNow()
                if ch in list_trim or ch in list_delim:
                    break
                _str += ch
            if self.lowercasemode:
                self.sval = _str.lower()
            else:
                self.sval = _str
            self.nval = None
            self.ttype = StreamTokenizer.TT_WORD
            return StreamTokenizer.TT_WORD

    def read(self):
        if self.pos >= self.end:
            self.ttype = StreamTokenizer.TT_EOF
            return False

        self.pos += 1
        if self.pos >= self.end:
            self.ttype = StreamTokenizer.TT_EOF
            return False
        else:
            return True

    def getNow(self):
        if self.pos >= self.end:
            return StreamTokenizer.TT_EOF
        else:
            return self.input[self.pos]


if __name__ == '__main__':
    import sys
    s = sys.stdin.readline()
    streamTokenizer = StreamTokenizer(s)
    streamTokenizer.nextToken()
    print(streamTokenizer.ttype)
