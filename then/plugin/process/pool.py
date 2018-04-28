import base64
import logging
import pickle
import sys
from multiprocessing.connection import Connection, Client, Listener
from setproctitle import setproctitle
from signal import signal, SIGINT
from subprocess import Popen
from typing import Type, Optional, Dict, List, Any, Tuple


def argv_encode(x):
    return base64.b64encode(pickle.dumps(x)).decode()


def argv_decode(x):
    return pickle.loads(base64.b64decode(x))


class TaskPoolWorker:
    def __init__(self, *args, **kwargs):
        self.running = True

    def stop(self):
        self.running = False

    def message(self, task):
        pass


def pool_worker_main(ident, addr, cls: Type[TaskPoolWorker], cls_args):
    running = True

    def stop(*args):
        nonlocal running
        running = False

    try:

        with Client(addr, family='AF_UNIX') as c:
            signal(SIGINT, stop)
            setproctitle(f'pool-{ident}')

            c.send(argv_encode(ident))

            w = cls(*cls_args)

            while running:
                x = argv_decode(c.recv())

                try:
                    rtn = w.message(x)

                    rtn2 = argv_encode(rtn)
                    c.send(rtn2)
                except Exception as e:
                    logging.getLogger(__name__).exception(f'{ident}')
                    raise
    except KeyboardInterrupt as e:
        logging.getLogger(__name__).error(f'[{ident}] Killed: {e}')
    except (ConnectionResetError, FileNotFoundError, EOFError) as e:
        logging.getLogger(__name__).error(f'[{ident}] Head disappeared: {e}')


class PatchedListener(Listener):
    def fileno(self):
        return self._listener._socket.fileno()


class TaskPoolChannel:
    def task_finished(self, task_id, task_payload):
        pass

    def child_started(self, child_id):
        pass


PId = str
JId = str


class TaskPool:
    def __init__(self, comm: TaskPoolChannel, listener: Optional[PatchedListener], cls: Type[TaskPoolWorker], cls_args):
        self.comm = comm
        self.listener = listener if listener else PatchedListener(family='AF_UNIX')
        self.child_ctr = 0

        self.cls = cls
        self.cls_args = cls_args

        self.processes = {}  # type: Dict[PId, Popen]
        self.comms_known = []  # type: List[Connection]
        self.comms = {}  # type: Dict[Connection, PId]

        self.pending = []  # type: List[Tuple[JId, Any]]

        # proc_id -> task_id
        self.assigned = {}  # type: Dict[PId, JId]

    def child_start_many(self, count):
        for _ in range(count):
            self.child_start()

    def child_start(self):
        ident = PId(self.child_ctr)
        self.child_ctr += 1

        args = (
            ident,
            self.listener.address,
            self.cls,
            self.cls_args
        )

        shellargs = [
            sys.executable,
            '-m',
            __name__,
            argv_encode(args)
        ]

        p = Popen(shellargs, stderr=sys.stderr, stdout=sys.stdout)

        self.processes[ident] = p

    @property
    def has_free(self):
        return len(self.assigned) < len(self.comms)

    def task_put(self, task_id, task):
        if not self.has_free:
            self.pending.append((task_id, task))
        else:
            self._task_assign(task_id, task)

    def _task_assign(self, task_id: JId, task):
        proc_id, chan = [(x, y) for y, x in self.comms.items() if x not in self.assigned][0]

        self.assigned[proc_id] = task_id
        chan.send(argv_encode(task))

    def _task_assign_pending(self):
        while len(self.pending) and self.has_free:
            self._task_assign(*self.pending.pop())

    def poll(self):
        return [self.listener] + self.comms_known

    def polled(self, pr: List[bool]):
        l, *c = pr

        if l:
            # todo we may start assigning tasks to the pool before we have enough workers.
            # so we need to handle that
            nc = self.listener.accept()
            self.comms_known.append(nc)
            c.append(True)

        for x in [x for x, y in zip(self.comms_known, c) if y]:
            # todo: x.recv may throw an exception if the subprocess had been killed
            # todo: this path will also never be taken if we do not subscribe to the 3rd argument of select.select
            x: Connection

            if not x.poll():
                continue

            bts = x.recv()

            if x not in self.comms:
                proc_id = argv_decode(bts)

                self.comms[x] = proc_id

                self.comm.child_started(proc_id)

                self._task_assign_pending()
            else:
                proc_id = self.comms[x]

                task_id = self.assigned[proc_id]
                task_payload = argv_decode(bts)

                del self.assigned[proc_id]

                self.comm.task_finished(task_id, task_payload)

                self._task_assign_pending()


if __name__ == '__main__':
    pool_worker_main(*argv_decode(sys.argv[1]))
