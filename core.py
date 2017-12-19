from threading import Thread, Lock, Condition, current_thread
from functools import reduce
from time import sleep


class RunnerHolder:

    def __init__(self):
        self.__map = {}

    def get(self):
        try:
            return self.__map[current_thread()]
        except KeyError:
            return None

    def set(self, val):
        self.__map.update({current_thread(): val})


class LockWrapper:

    def __init__(self):
        self.__condition = Condition(lock=Lock())

    def wait(self):
        self.__condition.wait()

    def notify(self):
        self.__condition.notify()

    def notify_all(self):
        self.__condition.notify_all()

    def acquire(self):
        acquired = False
        while not acquired:  # saves from interruption by POSIX signal
            acquired = self.__condition.acquire()

    def release(self):
        try:
            self.__condition.release()
        except RuntimeError:
            pass  # already released


class ComputationRunner:

    def __init__(self, runner):
        self.__runner = runner
        self.__lock = LockWrapper()
        self.__queue = []

    def add_task(self, task):
        wrapped_task = ComputationRunnerTask(self, task)
        self.__lock.acquire()
        self.__queue.append(wrapped_task)
        self.__lock.release()
        self.__runner.execute(wrapped_task.run)

    def remove_from_queue(self, task):
        self.__lock.acquire()
        try:
            self.__queue.remove(task)
        except ValueError:
            pass  # already removed
        self.__lock.release()

    def execute_pending_task(self):
        task = None
        self.__lock.acquire()
        if len(self.__queue) > 0:
            task = self.__queue[0]
            self.__queue = self.__queue[1:]
        self.__lock.release()
        if task is not None:
            task.run_internal()
        else:
            sleep(0.001)  # 1 ms


class ComputationRunnerTask:

    def __init__(self, runner, inner_task):
        self.__runner = runner
        self.__inner_task = inner_task
        self.__lock = LockWrapper()
        self.__started = False

    def run(self):
        global current_runner
        current_runner.set(self.__runner)
        self.run_internal()
        current_runner.set(None)

    def run_internal(self):
        self.__lock.acquire()
        should_run = not self.__started
        self.__started = True
        self.__runner.remove_from_queue(self)
        self.__lock.release()
        if should_run:
            self.__inner_task()


class Runner(Thread):

    def __init__(self):
        super().__init__(target=self.__run_impl, daemon=True)
        self.__lock = LockWrapper()
        self.__queue = []
        self.__should_finish = False
        self.start()

    def __run_impl(self):
        while True:
            self.__lock.acquire()
            if len(self.__queue) > 0:
                task = self.__queue[0]
                self.__queue = self.__queue[1:]
                self.__lock.release()
                task.execute()
            else:
                if self.__should_finish:
                    self.__lock.release()
                    break
                else:
                    self.__lock.wait()
                    self.__lock.release()

    def execute(self, runnable):
        self.__lock.acquire()
        future = FutureTask(runnable)
        self.__queue.append(future)
        self.__lock.notify()
        self.__lock.release()
        return future

    def finish(self):
        self.__should_finish = True


class FutureTask:
    def __init__(self, task):
        self.__lock = LockWrapper()
        self.__task = task
        self.__finished = False
        self.__result = None

    def execute(self):
        self.__lock.acquire()
        self.__result = self.__task()
        self.__finished = True
        self.__lock.notify_all()
        self.__lock.release()

    def get(self):
        self.__lock.acquire()
        while not self.is_finished():
            self.__lock.wait()
        self.__lock.release()
        return self.__result

    def is_finished(self):
        return self.__finished


class Computation:

    def __init__(self, func):
        self.__func = func

    def eval(self, val):
        return self.__func(val)

    def execute(self, val, runner):
        future = FutureTask(lambda: self.eval(val))
        runner.add_task(future.execute)
        return future

    def add(self, computation):
        if isinstance(computation, RunnerComputation):
            return RunnerComputationImpl(lambda x: self.eval(x), computation.runner)
        else:
            return Computation(lambda x: computation.eval(self.eval(x)))


class RunnerComputation(Computation):

    def __init__(self, runner):
        super().__init__(lambda x: x)
        self.runner = runner


class RunnerComputationImpl(Computation):

    def __init__(self, func, runner):
        super().__init__(func)
        self.__runner = runner

    def add(self, computation):
        if isinstance(computation, RunnerComputation):
            return RunnerComputationImpl(lambda x: self.eval(x), computation.runner)
        else:
            return RunnerComputationImpl(lambda x: wait_for_execution(computation.execute(self.eval(x),
                                                                                            self.__runner)),
                                           self.__runner)


def wait_for_execution(future):
    global current_runner
    while not future.is_finished():
        runner = current_runner.get()
        if runner is not None:
            runner.execute_pending_task()
    return future.get()


current_runner = RunnerHolder()


def value(val):
    return Counting(Computation(lambda x: val))


class Counting:

    def __init__(self, comp):
        self.__comp = comp

    def __next(self, func):
        return Counting(self.__comp.add(Computation(func)))

    def on(self, runner):
        return Counting(self.__comp.add(RunnerComputation(runner)))

    def compute(self, callback=None):
        if callback is not None:
            return self.then(callback).compute()
        else:
            return self.__comp.execute(None, ComputationRunner(Runner()))

    def then(self, func):
        return self.__next(func)

    def check(self, condition):
        return If(self, condition)

    def switch(self):
        return Switch(self)

    def every(self, *funcs):
        return self.then(lambda x: list(map(lambda f: f(x), funcs)))

    def join_values(self, vals):
        return self.then(lambda x: x + vals)

    def map(self, func):
        return self.then(lambda x: list(map(func, x)))

    def all(self, func):
        return self.map(func).then(lambda x: all(x))

    def any(self, func):
        return self.map(func).then(lambda x: any(x))

    def flat_map(self, func):
        return self.map(func).fold([], lambda x, y: x + y)

    def fold(self, initial, func):
        return self.then(lambda x: reduce(func, x, initial))

    def zip(self):
        return self.then(lambda x: list(zip(*x)))

    def unzip(self):
        return self.zip().map(lambda x: list(x)).then(lambda x: tuple(x))


class If:

    def __init__(self, counting, condition):
        self.__counting = counting
        self.__then = None
        self.__else = None
        self.__condition = condition

    def __decision_func(self, x):
        if self.__condition(x):
            return self.__then(x)
        else:
            return self.__else(x)

    def then(self, func):
        self.__then = lambda x: func(x)
        return self

    def els(self, func):
        self.__else = lambda x: func(x)
        return self.__counting.then(lambda x: self.__decision_func(x))


class Switch:

    def __init__(self, counting):
        self.__counting = counting
        self.__cases = []

    def case(self, val, func):
        self.__cases.append((val, func))
        return self

    def __select_func(self, x, default):
        for case in self.__cases:
            if case[0] == x:
                return case[1]
        return default

    def __select(self, x, default):
        func = self.__select_func(x, default)
        return func(x)

    def default(self, func):
        return self.__counting.then(lambda x: self.__select(x, func))
