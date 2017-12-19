from core import *


def log_fun(val):
    print(current_thread())
    return val


e = Runner()
e2 = Runner()
e3 = Runner()

e.execute(lambda: print("hello"))
e.execute(lambda: print("hello2"))
e.execute(lambda: print("hello3"))
e.execute(lambda: print("hello4")).get()

ex1 = ComputationRunner(e)
ex2 = ComputationRunner(e2)
ex3 = ComputationRunner(e3)

comp1 = Computation(lambda x: x * 2)
comp2 = Computation(lambda x: x * 2)
comp3 = Computation(log_fun)
comp4 = RunnerComputation(ex2)
comp5 = RunnerComputation(ex3)
comp7 = RunnerComputation(ex2)
comp6 = RunnerComputation(ex1)

log_fun(15)
v = comp1.add(comp2).add(comp3).add(comp3)\
    .add(comp4).add(comp3).add(comp3) \
    .add(comp5).add(comp3).add(comp3) \
    .add(comp6).add(comp3).add(comp3) \
    .execute(4, ex1).get()

print(v)


print()
print("Part 2")

print(value(5).then(lambda x: x * 2).compute().get())

value(5).then(lambda x: x + 5)\
    .check(lambda x: x < 15).then(lambda x: 10000).els(lambda x: x * 3)\
    .compute(lambda x: print(x)).get()

value(5).then(lambda x: x - 1).switch()\
    .case(4, lambda x: x + 3000)\
    .case(5, lambda x: x * 3)\
    .default(lambda x: x + 2)\
    .compute(lambda x: print(x)).get()

value(7).every(lambda x: x + 3, lambda x: x * 3, lambda x: x + 1000)\
    .then(lambda x: x[2])\
    .compute(lambda x: print(x)).get()

value([1]).join_values([8, 9, 10])\
    .map(lambda x: x * 2)\
    .all(lambda x: x > 1)\
    .compute(lambda x: print(x)).get()


value([1]).join_values([8, 9, 10])\
    .map(lambda x: x * 2)\
    .fold(0, lambda x, y: x + y)\
    .compute(lambda x: print(x)).get()

value([1]).join_values([8, 9, 10])\
    .map(lambda x: x * 2)\
    .flat_map(lambda x: [x, 1])\
    .compute(lambda x: print(x)).get()

value([1]).join_values([8, 9, 10])\
    .map(lambda x: x * 2)\
    .map(lambda x: [x, 1])\
    .compute(lambda x: print(x)).get()

value([1]).join_values([8, 9, 10])\
    .map(lambda x: x * 2)\
    .map(lambda x: [x, 1])\
    .zip()\
    .compute(lambda x: print(x)).get()

value([1]).join_values([8, 9, 10])\
    .map(lambda x: x * 2)\
    .map(lambda x: [x, 1])\
    .zip()\
    .unzip()\
    .compute(lambda x: print(x)).get()

value(1).then(lambda x: x + 1).on(ex1).then(lambda x: [x]).join_values([2, 4, 5])\
    .map(lambda x: x * x).compute(lambda x: print(x)).get()
