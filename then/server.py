import logging
import random
from typing import List, Dict, Optional, Tuple

from then.runtime import Capacity, WorkerState, Config, WaitingAck
from then.struct import Job, Id, Body, Filter, Structure
from then.util import none_get
from then.worker import NextJob


class JobKeeping:
    def __init__(self, worker_id: Id, job: Job):
        self.worker_id = worker_id
        self.job = job

    def __repr__(self):
        return f'JobKeeping({self.worker_id}, {self.job})'


class QueueCommChannel:
    def job(self, worker_id: Id, job: Job):
        assert False

    def job_finish_ack(self, worker_id: Id, job_id: Id):
        assert False


class QueueStateChannel:
    def worker_forget(self, worker_id: Id):
        assert False


class Queue:
    def __init__(self,
                 config: Config,
                 comm: QueueCommChannel,
                 state: QueueStateChannel,
                 matcher: Structure,

                 workers: Optional[Dict[Id, WorkerState]] = None,
                 pending: Optional[Dict[Id, Job]] = None,
                 running: Optional[Dict[Id, Job]] = None,
                 done: Optional[Dict[Id, Job]] = None,
                 pending_ack: Optional[Dict[Id, WaitingAck[JobKeeping]]] = None,
                 ):
        self.config = config

        # <currently> registered workers.
        self.workers = none_get(workers, {})  # type: Dict[Id, WorkerState]

        # jobs which didn't get a worker; yet
        self.pending = none_get(pending, {})  # type: Dict[Id, Job]
        self.running = none_get(running, {})  # type: Dict[Id, Job]
        self.done = none_get(done, {})  # type: Dict[Id, Job]

        self.pending_ack = none_get(pending_ack, {})  # type: Dict[Id, WaitingAck[JobKeeping]]

        self.comm = comm
        self.st = state

        self.matcher = matcher

    def serialize(self, workdir):
        pass

    @classmethod
    def deserialize(self, config: Config, channel: QueueCommChannel, matcher: Structure, workdir):
        pass

    def tick(self):
        worker_ids = [x for x in self.workers.keys()]

        for worker_id in worker_ids:
            w = self.workers[worker_id]

            w.pings_remaining -= 1

            if w.pings_remaining <= 0:
                # todo: log the worker timeout
                self.worker_unregister(worker_id)

        for x in WaitingAck.exec(self.pending_ack, min_acks=self.config.min_acks):
            x: JobKeeping
            self.comm.job(x.worker_id, x.job)

    def worker_register(self, worker: WorkerState):
        logging.getLogger('server.workers').error(f'Registered {worker}')
        if worker.id in self.workers:
            return

        worker.pings_remaining = self.config.max_pings

        self.workers[worker.id] = worker

        jobs_to_assign = [y for y in self.pending.values() if worker.filter.match(y.body)]
        for i, x in enumerate(jobs_to_assign):
            if i == worker.capacity.remaining:
                break

            self._job_assign(x)

    def worker_ping(self, worker_id: Id, filter: Filter, capacity: Capacity):
        if worker_id not in self.workers:
            self.worker_register(WorkerState(worker_id, filter, capacity, [], pings_remaining=self.config.max_pings))

        self._worker_capacity(worker_id, capacity)

        self.workers[worker_id].pings_remaining = self.config.max_pings

    def worker_unregister(self, worker_id: Id):
        logging.getLogger('server.workers').error(f'Unregistered {worker_id}')
        if worker_id not in self.workers:
            return

        to_resign = self.workers[worker_id].assigned

        for x in to_resign:
            self._job_resign(worker_id, x)

        to_remove_acks = [x for x, v in self.pending_ack.items() if v.x.worker_id == worker_id]

        for x in to_remove_acks:
            del self.pending_ack[x]

        del self.workers[worker_id]

        self.st.worker_forget(worker_id)

    def worker_job_ack(self, worker_id: Id, capacity: Capacity, job_id: Id):
        logging.getLogger('server.trace.ack').debug(f'{worker_id} {job_id}')
        # the job had been received by the worker
        if worker_id not in self.workers:
            return

        self._worker_capacity(worker_id, capacity)

        if job_id in self.workers[worker_id].assigned:
            if job_id in self.pending_ack:
                del self.pending_ack[job_id]

    def worker_job_nack(self, worker_id: Id, capacity: Capacity, job_id: Id):
        logging.getLogger('server.trace.nack').debug(f'{worker_id} {job_id}')
        if worker_id not in self.workers:
            return

        self._worker_capacity(worker_id, capacity)

        if job_id in self.workers[worker_id].assigned:
            j = self._job_resign(worker_id, job_id)
            self._job_assign(j)

    def worker_job_finish(self, worker_id: Id, capacity: Capacity, job_id: Id, payloads: List[NextJob]):
        logging.getLogger('server.trace.finish').debug(f'{worker_id} {job_id}')
        # stop worker spamming us if it was demoted
        self.comm.job_finish_ack(worker_id, job_id)

        # if a worker was unregistered since we no longer should have assigned to job to it
        if worker_id not in self.workers:
            return

        self._worker_capacity(worker_id, capacity)

        logging.getLogger('server.trace').debug(f'0 {worker_id} {job_id}')

        if job_id in self.workers[worker_id].assigned:
            logging.getLogger('server.trace').debug(f'1 {worker_id} {job_id}')

            j = self._job_resign(worker_id, job_id)

            self._job_done(j)

            pending_keys = [x for x in self.pending.keys()]
            for k in pending_keys:
                self._job_assign(self.pending[k])

            for payload in payloads:
                # todo: needed id here (maybe workers could use them instead)
                self.task_match(payload.id, payload.task)

    def _worker_capacity(self, worker_id: Id, capacity: Capacity):
        worker = self.workers[worker_id]

        if worker.capacity.version < capacity.version:
            worker.capacity = capacity

    def _job_known(self, id: Id):
        return id in self.pending or id in self.running or id in self.done

    def _job_resign(self, worker_id: Id, job_id: Id):
        self.workers[worker_id].resign(job_id)
        try:
            r = self.running[job_id]
            del self.running[job_id]
            return r
        except KeyError:
            # todo: send a task to worker that it acknowledges but never completes
            # todo: kill the worker
            # todo: thsi should happen while resigning a task
            print(self.running)
            raise

    def _job_assign(self, job: Job):
        if job.id in self.pending:
            del self.pending[job.id]

        if job.id in self.done:
            logging.getLogger('server.job.done').debug(job)
            return

        selected = list(self.workers.values())

        def _random_start(selected):
            idx_start = random.randint(0, len(selected) - 1)

            for i in range(idx_start, len(selected)):
                yield selected[i]

            for i in range(0, idx_start):
                yield selected[i]

        for worker in _random_start(selected):
            if worker.capacity.is_available and worker.filter.match(job.body):
                self.running[job.id] = job
                self.workers[worker.id].assign(job.id)
                self.workers[worker.id].capacity.current += 1

                self.comm.job(worker.id, job)
                self.pending_ack[job.id] = WaitingAck(JobKeeping(worker.id, job), self.config.min_acks)

                break

        if not self._job_known(job.id):
            logging.getLogger('server.job.pending').debug(job)
            self.pending[job.id] = job
        else:
            logging.getLogger('server.job.match').debug(job)

    def _job_done(self, job: Job):
        logging.getLogger('server.job.done').debug(job)
        self.done[job.id] = job

    def task_match(self, id: Id, task: Body):
        if self._job_known(id):
            return

        for i, rule in enumerate(self.matcher.match(task)):
            self._job_assign(Job(Id(id.id + '/' + str(i)), rule, task))
