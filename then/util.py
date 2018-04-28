from typing import TypeVar, Optional

T = TypeVar('T')


def none_get(x: Optional[T], default: T) -> T:
    if x is not None:
        return x
    else:
        return default
