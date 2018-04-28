import json
import struct


def deserialize_json(j):
    return json.loads(j)


def serialize_json(j):
    return json.dumps(j, separators=(',', ':')).encode()


def pack_bytes(bts):
    len_bts = struct.pack('I', len(bts))

    return len_bts + bts


def unpack_bytes(bts):
    buffer = memoryview(bts)

    if len(buffer) < 4:
        raise BlockingIOError()
    size, = struct.unpack('I', buffer[:4])
    if len(buffer) < 4 + size:
        raise BlockingIOError()

    return buffer[4:size + 4].tobytes()
