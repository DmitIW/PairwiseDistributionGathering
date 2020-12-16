from typing import Generator, Any, Callable, NoReturn
from functools import partial

from processing.storage import Storage


def get_and_store(data_flow: Generator[Any, Any, None], storage: Storage, callback: Callable[..., Any],
                  **kwargs) -> NoReturn:
    car_callback = partial(callback, **kwargs)
    for data in data_flow:
        storage.store(car_callback(data))
