import logging
from typing import List, Dict, Optional, TypeVar, Type

from then.impl.backend.udp.util import Pollable
from then.runtime import Capacity, Config, WaitingAck
from then.struct import Id, Body, Job, Filter


class NextJob:
    def __init__(self, id: Id, task: Body):
        self.id = id
        self.task = task


class WorkerPluginChannel:
    '''
    Passed to the plugin so that plugin could notify of the job finish
    '''

    def job_finished(self, job_id: Id, payloads: List[NextJob]):
        raise NotImplementedError('')


T = TypeVar('T', bound='WorkerPlugin')


class WorkerPlugin(Pollable):
    @classmethod
    def init(cls: Type[T], comm: WorkerPluginChannel, config: Body) -> T:
        raise NotImplementedError('')

    def job_assigned(self, job: Job):
        raise NotImplementedError('')


class JobKeeping:
    def __init__(self, job_id: Id, payloads: List[NextJob]):
        self.job_id = job_id
        self.payloads = payloads


class WorkerChannel:
    def ping(self, filter: Filter, capacity: Capacity):
        pass

    def job_ack(self, capacity: Capacity, job_id: Id):
        pass

    def job_nack(self, capacity: Capacity, job_id: Id):
        pass

    def job_finish(self, capacity: Capacity, job_id: Id, payloads: List[NextJob]):
        pass


class Worker:
    def __init__(
            self,
            config: Config,
            comm: WorkerChannel,
            pool: WorkerPlugin,
            filter: Filter,
            max_capacity: Optional[int] = None):
        self.filter = filter
        self.config = config
        self.capacity = Capacity(0, 0, max_capacity)

        self.running = {}  # type: Dict[Id, Job]
        self.done = {}  # type: Dict[Id, Job]

        self.ping_ticks = 0

        self.pending_ack = {}  # type: Dict[Id, WaitingAck[JobKeeping]]

        self.comm = comm
        self.pool = pool

    def tick(self):
        self.ping_ticks -= 1

        if self.ping_ticks < 0:
            self.ping_ticks = self.config.min_pings

            self.comm.ping(self.filter, self.capacity)

        for x in WaitingAck.exec(self.pending_ack, min_acks=self.config.min_acks):
            x: JobKeeping
            self.comm.job_finish(self.capacity, x.job_id, x.payloads)

    def job_finish(self, job_id: Id, payloads: List[NextJob]):
        logging.getLogger('worker.trace.finish').debug(f'{job_id}')
        self.capacity.down()

        job = self.running[job_id]
        del self.running[job_id]
        self.done[job_id] = job

        self.pending_ack[job_id] = WaitingAck(JobKeeping(job_id, payloads), self.config.min_acks)

        self.comm.job_finish(self.capacity, job_id, payloads)

    def job_finish_ack(self, job_id: Id):
        logging.getLogger('worker.trace.finish_ack').debug(f'{job_id}')
        if job_id in self.pending_ack:
            del self.pending_ack[job_id]

    def job_assign(self, job: Job):
        logging.getLogger('worker.trace.assign').debug(f'{job}')
        if self._job_known(job.id):
            self.comm.job_ack(self.capacity, job.id)
            return

        if not self.capacity.is_available:
            self.comm.job_nack(self.capacity, job.id)
            return

        self.pool.job_assigned(job)

        self.running[job.id] = job

        self.capacity.up()

        self.comm.job_ack(self.capacity, job.id)

    def _job_known(self, job_id: Id):
        return job_id in self.running or job_id in self.done
