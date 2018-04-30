import argparse
import logging
import os
import socket
from typing import Dict, Optional, List

from then.impl.backend.udp.framework import PollingUnit
from then.impl.backend.udp.util import Address, select_helper, _recv_parse_buffer
from then.impl.frontend.tcp.server import TCPFrontendServer
from then.impl.serde.entity import capacity_json_from
from then.impl.serde.util import deserialize_json, serialize_json
from then.server import Queue, QueueStateChannel, QueueCommChannel
from then.runtime import Config, Capacity
from then.struct import Structure, Id, Job, Filter, Body
from then.worker import NextJob


class UDPQueueStateChannel(QueueStateChannel):
    def __init__(self, known_hosts):
        self.known_hosts = known_hosts

    def worker_forget(self, worker_id: Id):
        if worker_id in self.known_hosts:
            del self.known_hosts[worker_id]


class UDPQueueCommChannel(QueueCommChannel):
    def __init__(self, socket, known_hosts):
        self.known_hosts = known_hosts
        self.socket = socket

    def job(self, worker_id: Id, job: Job):
        if worker_id in self.known_hosts:
            self.known_hosts[worker_id].send(
                self.socket,
                serialize_json({
                    't': 'j',
                    'i': job.id.id,
                    'r': job.rule.val,
                    'b': job.task.val,
                })
            )

    def job_finish_ack(self, worker_id: Id, job_id: Id):
        if worker_id in self.known_hosts:
            self.known_hosts[worker_id].send(
                self.socket,
                serialize_json({
                    't': 'jfa',
                    'i': job_id.id,
                })
            )


def set_logging():
    import sys
    logger = logging.getLogger()
    ch = logging.StreamHandler(sys.stderr)

    format = logging.Formatter("%(asctime)s\t%(levelname)s\t%(name)s\t%(message)s")
    ch.setFormatter(format)
    ch.setLevel(logging.DEBUG)
    logger.addHandler(ch)


def server(herz, ttl, wait, workdir, listen, port):
    # we initialize the matcher types, etc. in one single entity. <in general>

    set_logging()

    logging.getLogger('server').setLevel(logging.DEBUG)

    known_hosts = {}  # type: Dict[Id, Address]

    sock_recv = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock_recv.bind((listen, port))
    sock_recv.settimeout(0)

    sock_send = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    c = Config(
        max_pings=ttl,
        min_acks=wait,
        herz=herz
    )
    s = Structure(os.path.join(workdir, 'triggers'))

    q = Queue(
        c,
        UDPQueueCommChannel(sock_send, known_hosts),
        UDPQueueStateChannel(known_hosts),
        s,
    )

    tfs = TCPFrontendServer(
        '127.0.0.1',
        9870,
        s,
        q,
    )

    class ServerPollingUnit(PollingUnit):
        def poll(self, max_sleep) -> Optional[List[bool]]:
            return select_helper([sock_recv] + tfs.poll(), max_sleep)

        def tick(self):
            q.tick()

        def polled(self, pr: List[bool]):
            s_mg, *other = pr

            if s_mg:
                for addr, buffer in _recv_parse_buffer(sock_recv):
                    msg = deserialize_json(buffer)

                    addr = Address(*addr)

                    known_hosts[Id(msg['w'])] = addr

                    if msg['t'] == 'p':
                        q.worker_ping(Id(msg['w']), Filter(Body(msg['f'])), capacity_json_from(msg))
                    elif msg['t'] == 'ja':
                        q.worker_job_ack(Id(msg['w']), capacity_json_from(msg), Id(msg['ji']))
                    elif msg['t'] == 'jn':
                        q.worker_job_nack(Id(msg['w']), capacity_json_from(msg), Id(msg['ji']))
                    elif msg['t'] == 'jf':
                        q.worker_job_finish(Id(msg['w']), capacity_json_from(msg), Id(msg['ji']),
                                            [NextJob(Id(x['i']), Body(x['t'])) for x in msg['ps']])
                    else:
                        assert False, (addr, msg)

            if any(other):
                tfs.polled(other)

    p = ServerPollingUnit(1. / c.herz)

    p.run()

    # todo: on exit, save the current ``Queue`` state into a file
    # todo: on entry, restore the current ``Queue`` state from a file


def prepare_argparse():
    parser = argparse.ArgumentParser(description='then server')
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
        '--dir',
        dest='workdir',
        default='./',
        help='working dir (default: "%(default)s")'
    )

    parser.add_argument(
        '--listen',
        dest='listen',
        default='127.0.0.1',
        help='listen address (default: "%(default)s")'
    )

    parser.add_argument(
        '--port',
        dest='port',
        default=46585,
        type=int,
        help='listen address (default: "%(default)s")'
    )

    return parser


if __name__ == '__main__':
    ns = vars(prepare_argparse().parse_args())
    server(**ns)
