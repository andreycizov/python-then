import select
from time import sleep

from then.plugin.process import TaskPool
from then.impl.backend.udp.util import select_helper
from then_tests.impl.base.test_pool2 import TestTaskPoolChannel, TestWorker


def test_task_pool():
    p = TaskPool(TestTaskPoolChannel(), None, TestWorker, tuple())

    p.child_start()
    p.child_start()
    p.child_start()
    p.child_start()
    p.child_start()
    p.child_start()
    p.child_start()
    p.child_start()
    p.child_start()
    p.child_start()
    p.child_start()
    p.child_start()
    p.child_start()
    p.child_start()
    p.child_start()
    p.child_start()
    p.child_start()

    p.task_put('j-1', 'asd')

    print(p)

    sleep(1)

    x, _, _ = select.select(p.poll(), [], [], 0)

    x = select_helper(p.poll(), 0)
    if x:
        p.polled(x)

    sleep(3)

    x = select_helper(p.poll(), 0)
    if x:
        p.polled(x)

    sleep(3)


if __name__ == '__main__':
    test_task_pool()
