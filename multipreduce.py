import os
import pickle
import time
from threading import Thread, Lock


class Reducer(object):

    def __init__(self, f, tasks=None, max_worker=10, encode=None, decode=None):
        '''The Reducer do computation `reduce(f, tasks)`, but in multiprocesses way.
        Apply f of two item of tasks in a child process, And the encoded result is pass
        back throngh a pipe, then decode the result and add it to tasks for next computation.
        PS The stdlib multiprocess is too hard to use.
        '''
        self.f = f
        self.max_worker = max_worker
        self.encode = encode or pickle.dumps
        self.decode = decode or pickle.loads
        
        if tasks:
            self.tasks = list(tasks)
        else:
            self.tasks = []
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
                if self.stopped and len(self.tasks) <= 1:
                        self.lock.release()
                        self._collect_done = True
                        return
                self.lock.release()
                time.sleep(.1)
                continue
            self.lock.release()
            pid, _ = os.wait()
            p = self.workers.pop(pid)
            s = p.read()
            p.close()
            self.result = self.decode(s)
            self.tasks.append(self.result)

    def feed(self, t):
        if self.stopped:
            return
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

    def get_result(self, now=False):
        if not now:
            while True:
                if self._collect_done:
                    break
                time.sleep(.1)
        if self.result is not None:
            return self.result
        try:
            return self.tasks[0]
        except IndexError:
            return None


def reduce(function, sequence, initial=None, **kwargs):
    if not callable(function):
        raise TypeError("%s object is not callable" % function)
    try:
        iter(sequence)
    except:
        raise
    if len(sequence) == 0:
        if not initial:
            raise TypeError('reduce() of empty sequence with no initial value')
        return initial
    if initial:
        sequence.append(initial)
    r = Reducer(function, sequence, **kwargs)
    r.start()
    r.stop()
    return r.get_result()


if __name__ == '__main__':

    import random
    seq = list(range(15))
    def add(x, y):
        time.sleep(random.random())
        return x+y

    t0 = time.time()
    r = reduce(add, seq)
    t1 = time.time()
    print(r, reduce, t1-t0)

    try:
        reduce = __builtins__.reduce
    except:
        from functools import reduce
    t0 = time.time()
    r = reduce(add, seq)
    t1 = time.time()
    print(r, reduce, t1-t0)
