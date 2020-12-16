from typing import Generator, Any, Callable, NoReturn, Iterator
from functools import partial

from processing.storage import AStorage, Storage


def get_and_store(data_flow: Generator[Any, Any, None], storage: Storage, callback: Callable[..., Any],
                  **kwargs) -> NoReturn:
    car_callback = partial(callback, **kwargs)
    for data in data_flow:
        storage.store(car_callback(data))


async def flow_processing_task(data_flow: Iterator, storage: AStorage) -> NoReturn:
    for data in data_flow:
        await storage.store(data)
