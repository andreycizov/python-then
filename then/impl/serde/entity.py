from typing import Optional

from then.runtime import WorkerState, Capacity
from then.server import Worker
from then.struct import Rule, Id, Filter, Body


def rule_json_from(x):
    return Rule(Id(x['i']), Filter(Body(x['f'])), Body(x['b']))


def rule_json_to(x):
    return {'i': x.id.id, 'f': x.filter.body.val, 'b': x.body.val}


def worker_json_from(x):
    return Worker(Id(x['i']), Filter(Body(x['f'])))


def worker_json_to(x, cap: Optional[Capacity] = None):
    return {'i': x.id.id, 'f': x.filter.body.val, 'c': capacity_json_to(cap)}


def capacity_json_from(msg):
    if msg is None:
        return None

    return Capacity(msg['cv'], msg['cc'], msg['cm'])


def capacity_json_to(c: Optional[Capacity]):
    if c is None:
        return None

    return {
        'cc': c.current,
        'cm': c.max,
        'cv': c.version
    }
