import os
import pickle
import time
from threading import Thread, Lock
import logging


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
        # the lock now mainly for testing _loop round is done
        # in turn is actually for supporting feed
        # in other word, we don't neet Thread and Lock if feed removed.
        self.lock = Lock()

        self.result = None
        self._start()

    def process(self):
        while True:
            if len(self.workers) >= self.max_worker or len(self.tasks) < 2:
                yield
            if len(self.tasks) >= 2:
                x = self.tasks.pop()
                y = self.tasks.pop()
                r, w = os.pipe()
                pid = os.fork()
                if pid > 0:
                    os.close(w)
                    self.workers[pid] = os.fdopen(r, 'rb')
                elif pid == 0:
                    # handle all possible exception, should more specially
                    try:
                        os.close(r)
                        z = self.f(x, y)
                        w = os.fdopen(w, 'wb')
                        w.write(self.encode(z))
                        w.close()
                    except Exception as e:
                        logging.warning('worker error: %s', e)
                    exit(0)

    def collect(self):
        while True:
            if len(self.workers) == 0:
                yield
            if len(self.workers) > 0:
                pid, _ = os.wait()
                if pid not in self.workers:
                    continue
                p = self.workers.pop(pid)
                # handle all possible exception, should more specially
                try:
                    s = p.read()
                    p.close()
                    if s == '':
                        logging.warning('collect read None')
                        continue
                    self.result = self.decode(s)
                    self.tasks.append(self.result)
                    yield
                except Exception as e:
                    logging.warning('collect error: %s', e)

    def _loop(self):
        p = self.process()
        c = self.collect()
        while True:
            with self.lock:
                p.send(None)
                c.send(None)
            if self._is_done():
                time.sleep(.1)

    def _start(self):
        t = Thread(target=self._loop)
        t.daemon = True
        t.start()

    def _is_done(self):
        return len(self.tasks) < 2 and len(self.workers) == 0

    def feed(self, t):
        self.tasks.append(t)

    def get_result(self, now=False):
        if not now:
            while True:
                with self.lock:
                    if self._is_done():
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
