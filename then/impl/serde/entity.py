from then.runtime import WorkerState, Capacity
from then.struct import Rule, Id, Filter, Body


def rule_json_from(x):
    return Rule(Id(x['i']), Filter(Body(x['f'])), Body(x['b']))


def capacity_json_from(msg):
    return Capacity(msg['cv'], msg['cc'], msg['cm'])
