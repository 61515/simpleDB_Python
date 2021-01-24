from simpledb.util.Synchronized import synchronized
import threading


class Run:

    def __init__(self):
        self.lock = threading.Lock()  # 同步锁
        self.sum = 0

    @synchronized
    def add(self):
        for i in range(10000):
            self.sum += 1
            print("add " + str(self.sum))

    @synchronized
    def sub(self):
        for i in range(10000):
            self.sum -= 1
            print("sub " + str(self.sum))

    def cal(self):
        t1 = threading.Thread(target=self.add)
        t2 = threading.Thread(target=self.sub)
        t1.start()
        t2.start()


if __name__ == '__main__':
    run = Run()
    run.cal()
