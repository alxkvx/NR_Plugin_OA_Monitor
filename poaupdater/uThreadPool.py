import threading
import uLogging

consoleLock = threading.Lock()


def tprint(s):
    name = threading.currentThread().getName()
    consoleLock.acquire()
    uLogging.debug("[%s]	%s" % (name, s))
    consoleLock.release()

# TODO replace with standard ThreadPoolExecutor on RHEL5 (and python 2.4) EOL


class ThreadPool:

    def __init__(self, workerFun):
        self.workerFun = workerFun
        self.inputQ = []		# task item queue
        self.outputQ = []		# result queue
        self.lock = threading.Lock()
        self.condition = threading.Condition(lock=self.lock)
        self.tasks_number = 0		# number of tasks in process now
        self.terminateFlag = False

    def poolWorkerFun(self):
        tprint('pool worker started')
        i = self.get()
        while i != None:
            tprint('processing item %s' % str(i))
            res = None
            try:
                res = self.workerFun(i)
            except Exception, e:
                res = e
            self.task_done(i, res)
            tprint('processed')
            i = self.get()
        tprint('pool worker finished')

    def start(self, N):
        self.threads = []
        for x in range(N):
            t = threading.Thread(target=self.poolWorkerFun)
            t.start()
            self.threads.append(t)

    def put(self, item):
        self.condition.acquire()
        self.inputQ.append(item)
        self.tasks_number += 1
        self.condition.notifyAll()
        self.condition.release()

    # get next item for execution
    def get(self):
        self.condition.acquire()
        try:
            if self.terminateFlag:
                return None
            while not self.inputQ:
                self.condition.wait(1.0)  # we may miss notify() when processing result. so wake up regularly
                if self.terminateFlag:	  # if we woken up by terminate()
                    return None
            i = self.inputQ.pop(0)
            return i
        finally:
            self.condition.release()

    def task_done(self, item, result):
        self.condition.acquire()
        self.outputQ.append((item, result))
        self.tasks_number -= 1
        self.condition.notifyAll()
        self.condition.release()

    # returns None if overall job is finished: all tasks processed and result returned
    def get_result(self):
        res = None
        self.condition.acquire()
        if self.outputQ:
            res = self.outputQ.pop(0)
        else:
            if self.tasks_number:		# some tasks still not processed
                while not self.outputQ:
                    self.condition.wait()
                res = self.outputQ.pop(0)
        self.condition.release()
        return res

    def terminate(self):
        self.condition.acquire()
        self.terminateLocked()

    def terminateLocked(self):
        self.terminateFlag = True
        self.condition.notifyAll()
        self.condition.release()
        for t in self.threads:
            t.join()


__all__ = ["ThreadPool"]
