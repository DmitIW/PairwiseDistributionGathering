import asyncio
import time

from data_gathering.config import init
from data_gathering.base.time_utility import current_time_str, current_time
from data_gathering.tarantool_agent import TarantoolUpsertCL
from data_gathering.clickhouse_agent.community_labeling import CHCommLabQuery, S2DComm
from data_gathering.union.flow import Flow
from data_gathering.async_task import task


async def main():
    config = init()
    clickhouse_config = config["clickhouse_connection"]
    tarantool_config = config["tarantool_connection"]
    time_offset = config["time_offset"]
    updating_interval = config["updating_interval"]

    work_queue = asyncio.Queue()

    clickhouse_config["result_model"] = S2DComm
    ch_query = CHCommLabQuery(**clickhouse_config)

    tarantool_config["space_name"] = "comm_labeled"
    tn_upsert = TarantoolUpsertCL(**tarantool_config)

    flow = Flow(ch_query, tn_upsert)

    clickhouse_context = {
        "time_offset": time_offset,
    }
    flow = (flow, clickhouse_context)

    while True:
        start_time = current_time()
        print(f"{current_time_str()}::main: start processing")

        await work_queue.put(flow)

        tasks = [asyncio.create_task(task(name, work_queue)) for name in ("First")]
        await asyncio.gather(*tasks)
        elapsed_time = current_time() - start_time
        print(f"{current_time_str()}::main: finish processing; elapsed time: {elapsed_time} seconds")
        sleep = max(0, updating_interval - elapsed_time)
        print(f"{current_time_str()}::main: next processing after {sleep} seconds")
        time.sleep(sleep)


if __name__ == '__main__':
    print(f"{current_time_str()}::Pairwise distribution start gathering")
    asyncio.run(main())
    print(f"{current_time_str()}::Pairwise distribution stop gathering")
