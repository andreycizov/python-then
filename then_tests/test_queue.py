from typing import List, Tuple

from then.server import Queue, QueueCommChannel, Structure, QueueStateChannel
from then.runtime import Config, Capacity
from then.struct import Body, Job, Filter, Id


class TestChannelProxy:
    def __init__(self):
        self.queue = []

    def save(self, item, *args, **kwargs):
        self.queue.append((item, args, kwargs))

    def load(self):
        r = self.queue
        self.queue = []
        return r


class TestQueueChannel(QueueCommChannel, TestChannelProxy):
    def job(self, worker_id: Id, job: Job):
        self.save('job', worker_id, job)

    def job_finish_ack(self, worker_id: Id, job_id: Id):
        self.save('job_finish_ack', worker_id, job_id)


class TestQueueStateChannel(QueueStateChannel, TestChannelProxy):
    def worker_forget(self, worker_id: Id):
        self.save('worker_forget', worker_id)


# class TestMatcher(Matcher):
#     def match(self, body: Body) -> List[Body]:
#         return [
#             Body({'x': 0}),
#             Body({'x': 1}),
#         ]


def test_queue():
    c = TestQueueChannel()
    cs = TestQueueStateChannel()
    m = Structure.deserialize('./triggers')
    q = Queue(
        Config(3, 1),
        c,
        cs,
        m,
    )

    q.task_match(Id('j-1'), Body({'a': 'f', 'b': 'd'}))

    q.worker_ping(Id('w-1'), Filter(Body({'type': 'Const', 'body': {'a': 'f'}})), Capacity(0, 0, 5))

    print(q.workers)

    q.tick()
    q.tick()
    print(q.pending_ack)
    q.tick()

    print(q.pending_ack)

    print(c.load())
    print(cs.load())

    print(q.workers)


if __name__ == '__main__':
    test_queue()
