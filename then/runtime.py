from typing import Optional, List, Dict, TypeVar, Generic, Callable, Iterable

from then.struct import Id, Filter, Job


class Config:
    def __init__(self, max_pings=3, min_acks=1, herz=50):
        self.max_pings = max_pings
        self.min_pings = 1
        self.min_acks = min_acks
        self.herz = herz


class Capacity:
    def __init__(self, version: int, current: int, max: Optional[int]):
        self.version = version
        self.current = current
        self.max = max

    @property
    def remaining(self) -> Optional[int]:
        return None if self.max is None else (self.max - self.current)

    @property
    def is_available(self):
        return self.max is None or self.current < self.max

    def up(self):
        self.version += 1
        self.current += 1

    def down(self):
        self.version += 1
        self.current -= 1

    def __repr__(self):
        return f'Cap({self.version}, {self.current}/{self.max if self.max else "-"})'


class WorkerState:
    # worker supports a subset of payloads, we could assign a function for it to be able to match payloads.
    def __init__(self, id: Id, filter: Filter, capacity: Capacity, assigned: List[Id], pings_remaining=0):
        self.id = id
        self.filter = filter
        self.capacity = capacity
        self.assigned = assigned

        self.pings_remaining = pings_remaining

    def assign(self, id: Id):
        self.assigned += [id]

    def resign(self, id: Id):
        self.assigned = [x for x in self.assigned if x != id]

    def __repr__(self):
        return f'WorkerState({self.id}, {self.filter}, {self.capacity}, {self.pings_remaining})'


T = TypeVar('T')


class WaitingAck(Generic[T]):
    def __init__(self, x: T, acks: int):
        self.x = x
        self.acks = acks

    @classmethod
    def exec(cls, pending_ack: Dict[Id, 'WaitingAck[T]'], min_acks=3) -> Iterable[T]:
        pending_ids = [x for x in pending_ack.keys()]

        for pending_id in pending_ids:
            item = pending_ack[pending_id]
            item.acks -= 1
            if item.acks <= 0:
                item.acks = min_acks

                yield item.x

    def __repr__(self):
        return f'WaitingAck({self.x}, {self.acks})'
