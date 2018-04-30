import uuid
from typing import List, Any

from then.plugin.process.pool import TaskPoolChannel, TaskPoolWorker, TaskPool
from then.struct import Id, Body, Job
from then.worker import WorkerPluginChannel, WorkerPlugin, NextJob


class ProcessTaskPoolChannel(TaskPoolChannel):
    def __init__(self, comm: WorkerPluginChannel):
        self.comm = comm

    def task_finished(self, task_id, task_payload: List[NextJob]):
        self.comm.job_finished(
            Id(task_id),
            task_payload
        )


class ProcessWorker(TaskPoolWorker):
    def message(self, task: Job) -> List[NextJob]:
        print('xzec', task)
        if task.task.val.get('b', None) == 'd':
            ctr = task.task.val.get('ctr')
            if ctr < 100:
                return [NextJob(Id(uuid.uuid4().hex), Body({'b': 'd', 'ctr': ctr + 1}))]
            else:
                return []
        else:
            return []


class ProcessWorkerPlugin(WorkerPlugin):
    def __init__(
            self,
            comm: WorkerPluginChannel,
            parallel: int,

    ):
        self.task_pool = TaskPool(
            ProcessTaskPoolChannel(comm),
            None,
            ProcessWorker,
            tuple()
        )

        self.task_pool.child_start_many(parallel)

    @classmethod
    def init(cls, comm: WorkerPluginChannel, config: Body) -> 'ProcessWorkerPlugin':
        return ProcessWorkerPlugin(comm, int(config.val.get('parallel', 32)))

    def job_assigned(self, job: Job):
        self.task_pool.task_put(job.id.id, job)

    def poll(self) -> List[Any]:
        return self.task_pool.poll()

    def polled(self, pr: List[bool]):
        self.task_pool.polled(pr)
