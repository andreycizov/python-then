import argparse
import logging
import socket
import uuid
from typing import List, Optional, Type

from then.impl.backend.udp.framework import PollingUnit
from then.impl.backend.udp.server import set_logging
from then.impl.backend.udp.util import Address, select_helper, import_dyn, _recv_parse_buffer
from then.impl.serde.util import deserialize_json, serialize_json
from then.runtime import Config, Capacity
from then.struct import Filter, Id, Body, Job
from then.worker import Worker, WorkerChannel, WorkerPlugin, WorkerPluginChannel, NextJob


class UDPWorkerChannel(WorkerChannel):
    def __init__(self, worker_id: Id, filter: Filter, socket: socket.socket, addr: Address):
        self.worker_id = worker_id
        self.filter = filter
        self.socket = socket
        self.addr = addr

    def ping(self, filter: Filter, capacity: Capacity):
        self.addr.send(
            self.socket,
            serialize_json({
                't': 'p',
                'w': self.worker_id.id,
                'f': self.filter.body.val,
                'cv': capacity.version,
                'cc': capacity.current,
                'cm': capacity.max,
            })
        )

    def job_ack(self, capacity: Capacity, job_id: Id):
        self.addr.send(
            self.socket,
            serialize_json({
                't': 'ja',
                'w': self.worker_id.id,
                'cv': capacity.version,
                'cc': capacity.current,
                'cm': capacity.max,
                'ji': job_id.id
            })
        )

    def job_nack(self, capacity: Capacity, job_id: Id):
        self.addr.send(
            self.socket,
            serialize_json({
                't': 'jn',
                'w': self.worker_id.id,
                'cv': capacity.version,
                'cc': capacity.current,
                'cm': capacity.max,
                'ji': job_id.id,
            })
        )

    def job_finish(self, capacity: Capacity, job_id: Id, payloads: List[NextJob]):
        assert isinstance(payloads, list), ('the job plugin is buggy', payloads)

        self.addr.send(
            self.socket,
            serialize_json({
                't': 'jf',
                'w': self.worker_id.id,
                'cv': capacity.version,
                'cc': capacity.current,
                'cm': capacity.max,
                'ji': job_id.id,
                'ps': [{'i': x.id.id, 't': x.task.val} for x in payloads],
            })
        )


class UDPWorkerPluginChannel(WorkerPluginChannel):
    def __init__(self, func_cap, uwc: UDPWorkerChannel):
        self.func_cap = func_cap
        self.uwc = uwc

    def job_finished(self, job_id: Id, payloads: List[NextJob]):
        for p in payloads:
            assert isinstance(p, NextJob), payloads
        self.uwc.job_finish(self.func_cap(), job_id, payloads)


def worker_bin(worker_id, herz, ttl, wait, workdir, max_capacity, listen, port, filter, worker_path, worker_config):
    # we initialize the matcher types, etc. in one single entity. <in general>
    sock_send = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock_send.settimeout(0)

    filter = Filter(Body({'type': 'Const', 'body': {}})) if filter is None else Filter(Body(filter))
    assert 'type' in filter.body.val
    assert 'body' in filter.body.val

    set_logging()

    logging.getLogger('worker').setLevel(logging.DEBUG)

    worker_id = Id(worker_id)

    c = Config(
        max_pings=ttl,
        min_acks=wait,
        herz=herz
    )

    wpc: Type[WorkerPlugin]
    w: Worker

    # todo: now the thing is that current worker impl favours the job-based queue
    # todo: the workers that just pass the jobs to the docker, for example - must have a separate impl.

    uwc = UDPWorkerChannel(worker_id, filter, sock_send, Address(listen, port))

    def func_capacity():
        return w.capacity

    worker_config = Body({}) if worker_config is None else Body(worker_config)

    wpc = import_dyn(worker_path)

    wp = wpc.init(UDPWorkerPluginChannel(func_capacity, uwc), worker_config)

    w = Worker(
        c,
        uwc,
        wp,
        filter,
        max_capacity
    )

    logging.getLogger('worker').error(f'Started as {worker_id}')
    logging.getLogger('worker').error(f'Connecting to {listen}:{port}')

    class ClientPollingUnit(PollingUnit):
        def poll(self, max_sleep) -> Optional[List[bool]]:
            return select_helper([sock_send] + wp.poll(), max_sleep)

        def tick(self):
            w.tick()

        def polled(self, pr: List[bool]):
            ssend, *others = pr

            if ssend:
                for addr, buffer in _recv_parse_buffer(sock_send):
                    # when we parse the packets - we may know of the mapping
                    msg = deserialize_json(buffer)

                    if msg['t'] == 'j':
                        w.job_assign(Job(Id(msg['i']), Body(msg['r']), Body(msg['b'])))
                    elif msg['t'] == 'jfa':
                        w.job_finish_ack(Id(msg['i']))
                    else:
                        assert False, (addr, msg)

            if any(others):
                wp.polled(others)

    p = ClientPollingUnit(1. / c.herz)

    try:
        p.run()
    except KeyboardInterrupt:
        logging.getLogger('worker').info('Stopping')
    except Exception as e:
        logging.getLogger('worker').exception('Must not happen')


def prepare_argparse():
    parser = argparse.ArgumentParser(description='then server')

    parser.add_argument(
        '-I',
        dest='worker_id',
        # todo: generate a random ID every time
        default=uuid.uuid4().hex,
        type=str,
        help='worker id (default: "%(default)s")'
    )

    parser.add_argument(
        '--herz',
        dest='herz',
        default=20,
        type=int,
        help='how many ticks per second we process (default: "%(default)s")'
    )

    parser.add_argument(
        '--ttl',
        dest='ttl',
        default=40,
        type=int,
        help='how many ticks it takes for the worker to time out (default: "%(default)s")'
    )

    parser.add_argument(
        '--wait',
        dest='wait',
        default=3,
        type=int,
        help='how many ticks to wait before resending the message (default: "%(default)s")'
    )

    parser.add_argument(
        '-C',
        dest='max_capacity',
        default=None,
        type=int,
        help='maximum capacity (default: "%(default)s")'
    )

    parser.add_argument(
        '--dir',
        dest='workdir',
        default='./',
        help='working dir (default: "%(default)s")'
    )

    parser.add_argument(
        '--filter',
        dest='filter',
        default=None,
        help='filter to use (default: "%(default)s")'
    )

    parser.add_argument(
        '-W',
        dest='worker_path',
        default='then.plugin.process.plugin.ProcessWorkerPlugin',
        help='worker plugin to use (default: "%(default)s")'
    )

    parser.add_argument(
        '-X',
        dest='worker_config',
        default=None,
        help='worker config to use (default: "%(default)s")'
    )

    parser.add_argument(
        '--listen',
        dest='listen',
        default='127.0.0.1',
        help='server listen address (default: "%(default)s")'
    )

    parser.add_argument(
        '--port',
        dest='port',
        default=46585,
        type=int,
        help='server listen port (default: "%(default)s")'
    )

    return parser


if __name__ == '__main__':
    ns = vars(prepare_argparse().parse_args())
    worker_bin(**ns)
