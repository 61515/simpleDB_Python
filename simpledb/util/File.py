import os


class File:
    def __init__(self, file):
        self.filename = file

    def getname(self):
        return self.filename

    def exists(self):
        if os.path.exists(self.filename):
            return True
        else:
            return False

    def mkdirs(self):
        os.makedirs(self.filename)

    def list(self):
        return os.listdir(self.filename)

    def delete(self):
        os.remove(self.filename)


if __name__ == '__main__':
    file = File("E:\\工程\\simpleDB_Python\\simpledb\\file")
    print(file.list())
    # print(file.list())
