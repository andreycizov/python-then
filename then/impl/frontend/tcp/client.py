import argparse
import logging
import socket
from typing import Optional, List, Tuple

from then.impl.backend.udp.server import set_logging
from then.impl.serde.entity import rule_json_from, worker_json_from, capacity_json_from
from then.impl.serde.util import deserialize_json, serialize_json, pack_bytes, unpack_bytes
from then.runtime import Capacity
from then.server import Worker
from then.struct import Body, Id, deserialize_yaml


class FrontendTCPClient:
    def __init__(self, host, port):
        self.host = host
        self.port = port

        self._socket: Optional[socket.socket] = None

    @property
    def socket(self):
        if self._socket is None:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._socket.connect((self.host, self.port))
        return self._socket

    def _send(self, pld):
        self.socket.send(pack_bytes(
            serialize_json(pld)
        ))

    def _recv(self):
        return deserialize_json(unpack_bytes(self.socket.recv(2 ** 22)))

    def worker_list(self) -> List[Tuple[Worker, Optional[Capacity]]]:
        self._send({'t': 'w'})

        return sorted([(worker_json_from(x), capacity_json_from(x['c'])) for x in self._recv()], key=lambda x: x[0].id)

    def rule_list(self):
        self._send({'t': 'l'})

        return sorted([rule_json_from(x) for x in self._recv()], key=lambda x: x.id)

    def rule_remove(self, id: Id) -> bool:
        self._send({'t': 'r', 'i': id.id})
        rep = self._recv()

        return rep['o']

    def job_add(self, id: Id, body: Body):
        self._send({'t': 'j', 'i': id.id, 'b': body})

        rep = self._recv()

        assert rep['i'] == id.id, (rep['i'], id.id)
        assert rep['o'], str(rep)

        return rep


def client_main(action, host, port, **kwargs):
    set_logging()

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # logging.getLogger('main').error(f'Connecting to {host}:{port}')
    s.connect((host, port))
    logging.getLogger('main').error(f'Connected to {host}:{port}')

    def send(b):
        s.send(pack_bytes(
            serialize_json(b)
        ))

    def recv():
        return deserialize_json(unpack_bytes(s.recv(2 ** 22)))

    if action == 'workers':
        send({'t': 'w'})

        for x in sorted([rule_json_from(x) for x in recv()], key=lambda x: x.id):
            print(x.id)
            print('\t', x.filter)
            print('\t', x.body)
    elif action == 'list':
        send({'t': 'l'})

        for x in sorted([rule_json_from(x) for x in recv()], key=lambda x: x.id):
            print(x.id)
            print('\t', x.filter)
            print('\t', x.body)
    elif action == 'remove':
        send({'t': 'r', 'i': kwargs['id']})
        rep = recv()

        assert rep['o'], str(rep)
    elif action == 'add':
        send({'t': 'j', 'i': kwargs['id'], 'b': deserialize_yaml(kwargs['job_body_path'])})

        rep = recv()

        assert rep['i'] == kwargs['id'], (rep['i'], kwargs['id'])
        assert rep['o'], str(rep)

        print('Added', kwargs['id'])
    else:
        raise NotImplementedError(action)


def prepare_argparse():
    parser = argparse.ArgumentParser(description='then client')

    parser.add_argument(
        '--host',
        dest='host',
        default='127.0.0.1',
        help='server listen address (default: %(default)s)'
    )

    parser.add_argument(
        '--port',
        dest='port',
        default=9870,
        type=int,
        help='server listen port (default: %(default)s)'
    )

    action_p = parser.add_subparsers(
        title="action",
        dest="action",

        help="which action to perform",
        metavar="ACTION",
    )
    action_p.required = True

    ms = action_p.add_parser('status', help=f'show server status')
    mw = action_p.add_parser('workers', help=f'show registered workers')
    cl = action_p.add_parser('list', help=f'list match rules')
    cr = action_p.add_parser('remove', help=f'remove match rule')
    cr.add_argument(
        dest='id',
        type=str,
        help='rule id to remove'
    )

    ja = action_p.add_parser('add', help=f'add match rule')
    ja.add_argument(
        dest='id',
        type=str,
        help='job id'
    )
    ja.add_argument(
        dest='job_body_path',
        type=str,
        help='job body path .yaml'
    )

    return parser


if __name__ == '__main__':
    ns = vars(prepare_argparse().parse_args())
    client_main(**ns)
