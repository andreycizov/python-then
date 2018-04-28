import argparse
import logging
import socket

from then.impl.backend.udp.server import set_logging
from then.impl.backend.udp.util import _recv_parse_buffer
from then.impl.serde.entity import rule_json_from
from then.impl.serde.util import deserialize_json, serialize_json, pack_bytes, unpack_bytes
from then.struct import Rule, Body, Filter, Id, deserialize_yaml


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
        default=1029,
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
