from functools import wraps
from typing import Callable


def return_int(func: Callable):
    @wraps(func)
    def _return_int(*args, **kwargs):
        result = func(*args, **kwargs)
        return int(result)

    return _return_int
