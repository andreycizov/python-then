import select
import socket
import struct
from importlib import import_module
from typing import Optional, List, Any

from then.impl.serde.util import pack_bytes


class Address:
    def __init__(self, addr, port):
        self.addr = addr
        self.port = port

    def send(self, sock: socket.socket, data):
        sock.sendto(pack_bytes(data), (self.addr, self.port))

    def __repr__(self):
        return f'Address({self.addr}:{self.port})'


def select_helper(fds, max_wait) -> Optional[List[bool]]:
    rd, _, er = select.select(fds, [], fds, max_wait)

    r = set(rd + er)

    return [fd in r for fd in fds] if r else None


def import_dyn(path: str):
    start, end = path.rsplit('.', 1)

    try:
        module = import_module(start)
        return getattr(module, end)
    except ModuleNotFoundError:
        raise ImportError(path) from None


class Pollable:
    def poll(self) -> List[Any]:
        '''
        Called for every poll call
        :return: a list of file descriptors to be polled, each requires a fileno method
        '''
        return []

    def polled(self, pr: List[bool]):
        '''
        Called for every poll call that returns anything
        :param pr: a mask of file descriptors that were previously returned by a call to poll, which one of them is active
        '''


def _recv_parse_buffer(socket):
    try:
        while True:
            buffer, addr = socket.recvfrom(2 ** 16)

            buffer = memoryview(buffer)

            if len(buffer) < 4:
                break
            size, = struct.unpack('I', buffer[:4])
            if len(buffer) < 4 + size:
                break

            yield addr, buffer[4:size + 4].tobytes()
    except BlockingIOError:
        return
