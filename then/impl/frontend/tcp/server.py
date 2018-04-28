import logging
import socket
from typing import List, Any

from then.impl.backend.udp.util import Pollable, _recv_parse_buffer
from then.impl.serde.util import deserialize_json, serialize_json, pack_bytes
from then.server import Queue
from then.struct import Structure, Id, Body


class TCPFrontendServer(Pollable):
    def __init__(self, addr, port, s: Structure, q: Queue):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setblocking(0)
        try:
            self.sock.bind((addr, port))
        except OSError as e:
            raise OSError(f'Could not bind to {addr}:{port}, reason: {e}') from None
        self.sock.listen(1)

        self.clients = []

        self.s = s
        self.q = q

    def poll(self) -> List[Any]:
        return [self.sock] + self.clients

    def polled(self, pr: List[bool]):
        s, *clients = pr

        if s:
            conn, addr = self.sock.accept()
            self.clients.append(conn)
            logging.getLogger('server.frontend').debug(f'client {addr}')

        if any(clients):
            for client_sock in [b for a, b in zip(clients, self.clients) if a]:
                # todo: ensure conn timeouts
                # todo: ensure conn timeout err handling
                # todo: ensure conn gc
                client_sock: socket.socket
                for _, buffer in _recv_parse_buffer(client_sock):
                    msg = deserialize_json(buffer)

                    if msg['t'] == 'j':
                        self.q.task_match(Id(msg['i']), Body(msg['b']))
                        client_sock.send(pack_bytes(serialize_json({'t': 'ja', 'i': msg['i'], 'o': True})))
                    elif msg['t'] == 'l':
                        client_sock.send(pack_bytes(serialize_json([
                            {'i': x.id.id, 'f': x.filter.body.body, 'b': x.body.body} for x in self.s.list()
                        ])))
                    else:
                        logging.getLogger('tcp_front').error(str(msg))