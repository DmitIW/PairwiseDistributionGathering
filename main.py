import sys
import signal
import time
import asyncio

from config import (
    init
)
from flow import (
    ClickhouseAgent
)

from flow import (
    create_flow
)

from processing import (
    flow_processing_task
)

from utility import (
    current_time, current_time_str
)


async def task(name: str, work_queue: asyncio.Queue):
    while not work_queue.empty():
        flow = await work_queue.get()

        with flow as context:
            source = context[0]
            destination = context[1]
            data_gen = map(lambda x: x.row()[:-1], source.exec_query(**flow.get_arguments()))
            await flow_processing_task(data_gen, destination)

        time_now = current_time_str()
        print(f"{time_now}::{name}:: OVER;")


async def main():
    cfg = init()
    flows = []
    for pair in cfg["pairs"]:

        source = pair[0]

        args = {**cfg["clickhouse_connection"], **{"query_model": source}}
        ch_agent = ClickhouseAgent(**args)

        for position, destination in enumerate(pair[1]):
            attack = position == cfg["attack_pos"]
            flows.append(
                create_flow(ch_agent, destination(**cfg["tarantool_connection"]),
                            time_offset=cfg["time_offset"], attack=attack))

    work_queue = asyncio.Queue()
    while True:
        start_time = current_time()
        print(f"MainLoop:info: start processing")

        for flow in flows:
            await work_queue.put(flow)

        await asyncio.gather(
            asyncio.create_task(task("First", work_queue)),
            asyncio.create_task(task("Second", work_queue)),
            asyncio.create_task(task("Third", work_queue)),
            asyncio.create_task(task("Fourth", work_queue)),
        )

        elapsed_time = current_time() - start_time
        sleep_time = max(0, cfg["updating_interval"] - elapsed_time)
        print(f"MainLoop:info: end processing; elapsed time: {elapsed_time} seconds")
        print(f"MainLoop:info: next processing iteration after: {sleep_time} seconds")

        await asyncio.sleep(sleep_time)


def exit_gracefully(signum, frame):
    sys.exit(0)


if __name__ == "__main__":
    print("Gathering start")
    signal.signal(signal.SIGINT, exit_gracefully)
    asyncio.run(main())
    print("Gathering end")
