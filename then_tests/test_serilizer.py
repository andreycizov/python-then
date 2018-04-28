from then.struct import Structure, Body
from then.server import *
from then.worker import *


def test_srzlr():
    a = Structure.deserialize('./triggers')
    print(a)

    print(a.match(Body({'ctr': 1})))
    print(a.match(Body({'ctr': True})))


if __name__ == '__main__':
    test_srzlr()
