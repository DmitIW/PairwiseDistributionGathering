from asyncio import Queue
from typing import Dict, Any, Tuple

from data_gathering.union import Flow
from data_gathering.base.time_utility import current_time_str

ClickhouseContext = Tuple[Flow, Dict[str, Any]]


async def task(task_name: str, work_queue: Queue):
    while not work_queue.empty():
        flow, kwargs = await work_queue.get()
        print(f"{current_time_str()}::{task_name}: start processing")
        flow = flow.prepare_clickhouse_query(**kwargs)
        await flow.start()
        print(f"{current_time_str()}::{task_name}: finish processing")
