import logging
import os
import random
import tempfile
from typing import List, Dict, Optional, Tuple, Iterable

import yaml

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


def _random_start(selected):
    selected = list(selected)
    tl = len(selected)

    if tl == 0:
        return

    idx_start = random.randint(0, tl - 1)

    for i in range(idx_start, tl):
        yield selected[i]

    for i in range(0, idx_start):
        yield selected[i]


class Worker:
    def __init__(self, id: Id, filter: Filter):
        self.id = id
        self.filter = filter


class Workers:
    def __init__(self, config: Config, workdir, tmpdir):
        self.config = config
        self.workdir = workdir
        self.tmpdir = tmpdir

        os.makedirs(self.workdir, exist_ok=True)
        os.makedirs(self.tmpdir, exist_ok=True)

        self.caps: Dict[Id, Capacity] = {}
        self.pings: Dict[Id, int] = {}

    def __contains__(self, item: Id):
        return os.path.exists(self._path(item))

    def tick(self) -> Iterable[Id]:
        items = [x for x in self]

        for x in items:
            self.pings[x.id] = self.pings.get(x.id, self.config.max_pings) - 1
            if self.pings[x.id] < 0:
                self.pings[x.id] = self.config.max_pings
                yield x.id

    def capacity_get(self, worker_id: Id):
        old = self.caps.get(worker_id, Capacity(-1, 0, 0))

        return old

    def capacity_set(self, worker_id: Id, new: Capacity):
        old = self.capacity_get(worker_id)

        new = new if old.version < new.version else old

        self.caps[worker_id] = new

    def capacity_inc(self, worker_id: Id):
        new = self.capacity_get(worker_id)

        new.current += 1

    def _list(self) -> Iterable[Id]:
        for x in os.listdir(self.workdir):
            if not x.startswith('.'):
                suff = '.yaml'
                if x.endswith(suff):
                    yield Id(x[:-len(suff)])

    def _path(self, worker_id: Id):
        return os.path.join(self.workdir, f'{worker_id.id}.yaml')

    def ping(self, worker_id: Id):
        self.pings[worker_id] = self.config.max_pings

    def register(self, worker_id: Id, filter: Filter, capacity: Capacity) -> Tuple[bool, bool]:
        contents = {
            'f': filter.body.val,
        }

        self.capacity_set(worker_id, capacity)
        self.ping(worker_id)

        # f_out, f_path = tempfile.mkstemp(dir=self.tmpdir)

        try:
            # with os.fdopen(f_out, mode='w') as f_out:
            with open(self._path(worker_id), mode='x') as f_out:
                yaml.dump(contents, f_out)

            # this is a <small> race condition
            # existed = worker_id in self
            # os.rename(f_path, self._path(worker_id))

            # return True, not existed
            return True, True
        except FileExistsError:
            with open(self._path(worker_id), 'r') as f_in:
                contents_old = yaml.load(f_in)

            if contents_old['f'] != contents['f']:
                try:
                    with open(self._path(worker_id), 'w') as f_out:
                        f_out.write(bytes)
                        yaml.dump(contents, f_out)

                    return True, True
                except IOError:
                    logging.getLogger('server.workers').error(f'{self._path(worker_id)} not longer exists')

                    return False, False
            return True, False

    def get(self, worker_id: Id) -> Optional[Worker]:
        try:
            with open(self._path(worker_id), 'r') as f_in:
                bts = yaml.load(f_in)

                fv = bts['f']

                return Worker(
                    worker_id,
                    Filter(Body(fv)),
                )
        except FileNotFoundError:
            logging.getLogger('server.workers').error(f'{self._path(worker_id)} could not find')
            return None
        except (KeyError, ValueError):
            logging.getLogger('server.workers').error(f'{self._path(worker_id)} could not parse')
            return None

    def __iter__(self) -> Iterable[Worker]:
        for id in _random_start(self._list()):
            it = self.get(id)
            if it is None:
                continue
            yield it

    def unregister(self, worker_id: Id) -> bool:
        try:
            os.unlink(self._path(worker_id))
            return True
        except FileNotFoundError:
            logging.getLogger('server.workers').error(f'{self._path(worker_id)} could not find')
            return False
        finally:
            if worker_id in self.caps:
                del self.caps[worker_id]

            if worker_id in self.pings:
                del self.pings[worker_id]


class Jobs:
    PENDING = 'pending'
    DONE = 'done'
    ASSIGNED = 'assigned'
    RUNNING = 'running'

    def __init__(self, config: Config, workdir, tmpdir):
        self.config = config
        self.workdir = workdir
        self.tmpdir = tmpdir

        os.makedirs(self.tmpdir, exist_ok=True)

        os.makedirs(os.path.join(self.workdir, self.PENDING), mode=0o700, exist_ok=True)
        os.makedirs(os.path.join(self.workdir, self.DONE), mode=0o700, exist_ok=True)
        os.makedirs(os.path.join(self.workdir, self.ASSIGNED), mode=0o700, exist_ok=True)
        os.makedirs(os.path.join(self.workdir, self.RUNNING), mode=0o700, exist_ok=True)

    def _path(self, context: str, jid: Id):
        jid = jid.id
        return os.path.join(self.workdir, context, f'{jid}.yaml')

    def _exists(self, context: str, jid: Id):
        return os.path.exists(self._path(context, jid))

    def __contains__(self, item: Id):
        return self._exists('pending', item) or \
               self._exists('done', item)

    def create(self, job: Job) -> bool:
        if job.id in self:
            return False

        f_out, f_path = tempfile.mkstemp(dir=self.tmpdir)

        try:
            # todo we may be able to create a pending job, yet then find it in done
            with os.fdopen(f_out, mode='w') as f_out:
                yaml.dump(
                    {
                        'r': job.rule.val,
                        't': job.task.val
                    },
                    f_out
                )

            os.rename(f_path, self._path(self.PENDING, job.id))

            if self._exists(self.DONE, job.id):
                os.remove(self._path(self.PENDING, job.id))
                return False
            else:
                return True

        finally:
            try:
                os.remove(f_path)
            except OSError:
                pass
                # logging.getLogger('server.jobs').error(f'[0] could not remove temp file {f_path}')

    def resign(self, job_id: Id, worker_id: Id) -> bool:
        # todo we may do a CAS instead

        try:
            os.unlink(self._path(self.ASSIGNED, job_id))
            return True
        except FileNotFoundError:
            try:
                os.unlink(self._path(self.RUNNING, job_id))
                return True
            except FileNotFoundError:
                return False

    def assign(self, job_id: Id, worker_id: Id) -> bool:
        if job_id not in self:
            logging.getLogger('server.jobs').error(f'[0] job unknown {job_id}')
            return False

        f_out, f_path = tempfile.mkstemp(dir=self.tmpdir)

        try:
            # todo the issue is that we can not move a file atomically
            # todo make sure there is no one assigning a task at the same time
            with os.fdopen(f_out, mode='w') as f_out:
                yaml.dump(
                    {
                        'w': worker_id.id
                    },
                    f_out
                )

            os.rename(f_path, self._path(self.ASSIGNED, job_id))

            if not self._exists(self.PENDING, job_id):
                # if someone had just moved the job from pending to DONE
                os.remove(self._path(self.ASSIGNED, job_id))
                return False
            else:
                return True
        finally:
            try:
                os.remove(f_path)
            except OSError:
                pass

    def running(self, job_id: Id, worker_id: Id) -> bool:
        if job_id not in self:
            logging.getLogger('server.jobs').error(f'[1] job unknown {job_id}')
            return False

        try:
            os.rename(self._path(self.ASSIGNED, job_id), self._path(self.RUNNING, job_id))
            return True
        except FileNotFoundError:
            logging.getLogger('server.jobs').error(f'[2] could not start running {job_id}')
            return False

    def _get_pending(self, job_id: Id) -> Optional[Job]:
        try:
            with open(self._path(self.PENDING, job_id), 'r') as f_in:
                obj = yaml.load(f_in)
                r = Job(job_id, Body(obj['r']), Body(obj['t']))

                return r
        except KeyError:
            logging.getLogger('server.jobs').error(f'[3] could not deser {job_id}')
            return None
        except FileNotFoundError:
            return None

    def _get_assigned(self, context, job_id: Id) -> Optional[Id]:
        try:
            with open(self._path(context, job_id), 'r') as f_in:
                obj = yaml.load(f_in)

                return Id(obj['w'])
        except KeyError:
            logging.getLogger('server.jobs').error(f'[3] could not deser {job_id}')
            return None
        except FileNotFoundError:
            return None

    def done(self, job_id: Id, subsequent: List[Job]) -> bool:
        job = self._get_pending(job_id)

        if job is None:
            return False

        f_out, f_path = tempfile.mkstemp(dir=self.tmpdir)

        try:
            # todo the issue is that we can not move a file atomically
            # todo make sure there is no one assigning a task at the same time
            with os.fdopen(f_out, mode='w') as f_out:
                yaml.dump(
                    {
                        'r': job.rule.val,
                        't': job.task.val,
                        's': [{'i': x.id.id, 'r': x.rule.val, 't': x.task.val} for x in subsequent]
                    },
                    f_out
                )

            os.rename(f_path, self._path(self.DONE, job_id))
            os.unlink(self._path(self.PENDING, job_id))
            os.unlink(self._path(self.RUNNING, job_id))

            for x in subsequent:
                self.create(x)

            return True
        finally:
            try:
                os.remove(f_path)
            except OSError:
                pass

    def repair_done(self):
        # todo: find all subsequent in DONE and check if they are KNOWN
        # todo: find all pending and check if any of them are DONE - remove them
        pass

        assigned = self.list_assigned()
        pending = self.list_pending()
        running = self.list_running()
        done = self._list(self.DONE)

        for job_id in done:
            if job_id in [x for x, _ in assigned]:
                os.unlink(self._path(self.ASSIGNED, job_id))
                logging.getLogger('server.jobs.repair.done').error(f'done but assigned {job_id}')
            if job_id in [x for x, _ in running]:
                os.unlink(self._path(self.RUNNING, job_id))
                logging.getLogger('server.jobs.repair.done').error(f'done but running {job_id}')
            if job_id in [x for x in pending]:
                logging.getLogger('server.jobs.repair.done').error(f'done but pending {job_id}')
                os.unlink(self._path(self.PENDING, job_id))

            with open(self._path(self.DONE, job_id)) as f_in:
                obj = yaml.load(f_in)

                subs = [(Id(x['i']), x) for x in obj['s']]

                for sub_id, sub in subs:
                    if sub_id not in self:
                        logging.getLogger('server.jobs.repair.done').error(f'done but unsubsq {job_id} {sub_id}')
                        self.create(Job(sub_id, Body(sub['r']), Body(sub['t'])))

    def repair_missing_worker(self, workers: Workers):
        for job_id, worker_id in self.list_assigned():
            if worker_id not in workers:
                self.resign(job_id, worker_id)
                logging.getLogger('server.jobs.repair.missing_worker').error(f'assigned {job_id} {worker_id}')

        for job_id, worker_id in self.list_running():
            if worker_id not in workers:
                logging.getLogger('server.jobs.repair.missing_worker').error(f'running {job_id} {worker_id}')

    def _list(self, context):
        files = os.listdir(os.path.join(self.workdir, context))
        files = [x for x in files if not x.startswith('.')]
        suffix = '.yaml'
        files = [x[:-len(suffix)] for x in files if x.endswith(suffix)]
        return [Id(x) for x in files]

    def list_assigned(self) -> List[Tuple[Id, Id]]:
        files = [(x, self._get_assigned(self.ASSIGNED, x)) for x in self._list(self.ASSIGNED)]

        return [(x, y) for x, y in files if y is not None]

    def list_running(self) -> List[Tuple[Id, Id]]:
        files = [(x, self._get_assigned(self.RUNNING, x)) for x in self._list(self.RUNNING)]

        return [(x, y) for x, y in files if y is not None]

    def list_pending(self):
        pending = [x for x in self._list(self.PENDING)]

        not_pending = self._list(self.ASSIGNED) + self._list(self.RUNNING)

        return [x for x in pending if x not in not_pending]


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

        # todo these guys are _not_ redundant against the pending in each of the workers.
        self.pending_ack = none_get(pending_ack, {})  # type: Dict[Id, WaitingAck[JobKeeping]]

        self.comm = comm
        self.st = state

        self.matcher = matcher

        # v2

        self.js = Jobs(config, './q/jobs/', './tmp')
        self.ws = Workers(config, './q/workers', './tmp')

        self.js.repair_done()
        self.js.repair_missing_worker(self.ws)

    def serialize(self, workdir):
        pass

    @classmethod
    def deserialize(self, config: Config, channel: QueueCommChannel, matcher: Structure, workdir):
        pass

    def tick(self):
        for worker_id in self.ws.tick():
            self.worker_unregister(worker_id)

        for job_id, worker_id in self.js.list_assigned():
            # todo wait for the required number of pings
            job = self.js._get_pending(job_id)

            if job:
                self.comm.job(worker_id, job)

    def worker_register(self, worker: WorkerState):
        exists, is_new = self.ws.register(worker.id, worker.filter, worker.capacity)

        if is_new:
            logging.getLogger('server.workers').error(f'Registered {worker}')

        if exists:

            jobs_to_assign = [y for y in self.js.list_pending()]
            jobs_to_assign1 = [self.js._get_pending(y) for y in jobs_to_assign]
            jobs_to_assign2: List[Job] = [y for y in jobs_to_assign1 if y]
            jobs_to_assign3 = [y for y in jobs_to_assign2 if worker.filter.match(y.body)]
            for i, x in enumerate(jobs_to_assign3):
                if i == worker.capacity.remaining:
                    break

                self.js.assign(x.id, worker.id)

    def worker_ping(self, worker_id: Id, filter: Filter, capacity: Capacity):
        self.worker_register(WorkerState(worker_id, filter, capacity, [], 0))

    def worker_unregister(self, worker_id: Id):
        if self.ws.unregister(worker_id):
            logging.getLogger('server.workers').error(f'Unregistered {worker_id}')

            x1 = [jid for jid, wid in self.js.list_assigned() if wid == worker_id]
            x2 = [jid for jid, wid in self.js.list_running() if wid == worker_id]

            for x in x1 + x2:
                self.js.resign(x, worker_id)
                self._job_assign(x)
        # todo to_remove_acks = [x for x, v in self.pending_ack.items() if v.x.worker_id == worker_id]

    def worker_job_ack(self, worker_id: Id, capacity: Capacity, job_id: Id):
        logging.getLogger('server.trace.ack').debug(f'{worker_id} {job_id}')
        self.js.running(job_id, worker_id)

        self.ws.capacity_set(worker_id, capacity)

    def worker_job_nack(self, worker_id: Id, capacity: Capacity, job_id: Id):
        logging.getLogger('server.trace.nack').debug(f'{worker_id} {job_id}')
        if worker_id not in self.workers:
            return

        if self.js.resign(job_id, worker_id):
            self._job_assign(job_id)

        self.ws.capacity_set(worker_id, capacity)

    def worker_job_finish(self, worker_id: Id, capacity: Capacity, job_id: Id, payloads: List[NextJob]):
        logging.getLogger('server.trace.finish').debug(f'{worker_id} {job_id}')
        # stop worker spamming us if it was demoted
        self.comm.job_finish_ack(worker_id, job_id)

        self.ws.capacity_set(worker_id, capacity)

        subs: List[Job] = []

        for x in payloads:
            subs += list(self._task_match(x.id, x.task))

        self.js.done(job_id, subs)

        # todo: notify of newer items for every sub

        for sub in subs:
            self._job_assign(sub.id)

    def _task_match(self, id: Id, task: Body):
        for i, rule in enumerate(self.matcher.match(task)):
            # todo: job creation is idempotent
            # todo: therefore if the matcher configuration had changed
            # todo: between attempts at creating a job
            # todo: then it may or may not be idempotent
            # todo: I don't personally think it's important
            # todo: 1) addition of a job will create a job at n+1
            # todo: 2) deletion of a match will forget to re-create the match
            yield Job(Id(id.id + '_' + str(i)), rule, task)

    def task_match(self, id: Id, task: Body):
        for x in self._task_match(id, task):
            self.js.create(x)
            self._job_assign(x.id)

    def _job_known(self, id: Id):
        return id in self.js

    def _job_assign(self, job_id: Id):
        # todo redo all

        job = self.js._get_pending(job_id)

        if job is None:
            logging.getLogger('server.job.pending').error(f'[1] unknown {job_id}')
            return

        is_assigned = False

        for worker in self.ws:
            cap = self.ws.capacity_get(worker.id)
            if cap.is_available and worker.filter.match(job.body):
                self.js.assign(job.id, worker.id)
                self.comm.job(worker.id, job)
                is_assigned = True
                break

        if is_assigned:
            logging.getLogger('server.job.pending').debug(job)
        else:
            logging.getLogger('server.job.match').debug(job)
