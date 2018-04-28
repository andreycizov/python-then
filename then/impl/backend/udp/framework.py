from datetime import datetime, timedelta
from typing import Optional, List


def td(seconds) -> timedelta:
    return timedelta(seconds=seconds)


def tn() -> datetime:
    return datetime.now()


class PollingUnit:
    def __init__(self, tick_delta=0.1):
        assert tick_delta > 0.
        self.tick_delta = tick_delta
        self.next_tick_time = tn() + td(self.tick_delta)

    def poll(self, max_sleep) -> Optional[List[bool]]:
        raise NotImplementedError('')

    def tick(self):
        raise NotImplementedError('')

    def polled(self, pr: List[bool]):
        raise NotImplementedError('')

    def run(self):
        while True:
            now_time = tn()
            dt = (self.next_tick_time - now_time).total_seconds()

            pr = self.poll(max(0., dt))
            if pr is not None:
                self.polled(pr)

            while dt < 0:
                self.tick()
                dt += self.tick_delta
                self.next_tick_time = self.next_tick_time + td(self.tick_delta)
