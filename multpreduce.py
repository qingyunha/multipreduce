import os
import pickle
import time
from threading import Thread, Lock


class Worker(object):

    def __init__(self, f, max_worker=10, tasks=None, encode=None, decode=None):
        '''The Worker do computation `reduce(f, tasks)`, but in multiprocesses way.
        Apply f of two item of tasks in a child process, And the encoded result is pass
        back to the Worker throngh a pipe, then decode it and add it to tasks for next 
        computation. PS The stdlib multiprocess is too hard to use.
        '''
        self.f = f
        self.max_worker = max_worker
        self.encode = encode or pickle.dumps
        self.decode = encode or pickle.loads
        
        self.tasks = tasks or []
        self.workers = {}
        self.lock = Lock()

        self._collect_done = False
        self.stopped = True
        self.result = None

    def process(self):
        while True:
            if self._collect_done:
                return
            if len(self.workers) >= self.max_worker or len(self.tasks) < 2:
                time.sleep(.1)
                continue
            with self.lock:
                x = self.tasks.pop()
                y = self.tasks.pop()
                r, w = os.pipe()
                pid = os.fork()
                if pid > 0:
                    os.close(w)
                    self.workers[pid] = os.fdopen(r, 'rb')
                elif pid == 0:
                    os.close(r)
                    z = self.f(x, y)
                    w = os.fdopen(w, 'wb')
                    w.write(self.encode(z))
                    exit(0)

    def collect(self):
        while True:
            self.lock.acquire()
            if len(self.workers) == 0:
                if len(self.tasks) <= 1:
                    try:
                        self.result = self.tasks[0]
                    except IndexError:
                        pass
                    if self.stopped:
                        self.lock.release()
                        self._collect_done = True
                        return
                self.lock.release()
                time.sleep(.1)
                continue
            self.lock.release()
            pid, _ = os.wait()
            p = self.workers[pid]
            s = p.read()
            p.close()
            self.tasks.append(self.decode(s))
            self.workers.pop(pid)

    def feed(self, tasks):
        if self.stopped:
            return
        try:
            tasks[0]
        except:
            tasks = [tasks]
        for t in tasks:
            self.tasks.append(t)

    def start(self):
        self.stopped = False
        self._p = Thread(target=self.process)
        self._p.daemon = True
        self._p.start()
        self._c = Thread(target=self.collect)
        self._c.daemon = True
        self._c.start()

    def stop(self):
        self.stopped = True

    def get_result(self):
        while True:
            if self.result:
                return self.result
            time.sleep(.1)


if __name__ == '__main__':

    import random
    T = list(range(15))
    def add(x, y):
        time.sleep(random.random())
        return x+y
    t0 = time.time()
    worker = Worker(add)
    worker.start()
    worker.feed(T)
    worker.stop()
    print(worker.get_result())
    t1 = time.time()
    print('------------', t1-t0)

    t0 = time.time()
    r = 0
    for i in T:
        r = add(r, i)
    t1 = time.time()
    print(r)
    print('------------', t1-t0)
