import asyncio
import time

from data_gathering.base import current_time_str
from data_gathering.base.time_utility import current_time
from data_gathering.config import init
from data_gathering.union import Flow
from data_gathering.clickhouse_agent import ClickhouseQuery
from data_gathering.tarantool_agent import TarantoolUpsert
from data_gathering.async_task import task

from data_gathering.clickhouse_agent.pair_wise_distribution import Src2Dst, Dst2Src, Dst2Proto

pairs = (
    (Src2Dst, "src2dst_at", True),
    (Src2Dst, "src2dst_lg", False),
    (Dst2Src, "dst2src_at", True),
    (Dst2Src, "dst2src_lg", False),
    (Dst2Proto, "dst2proto_at", True),
    (Dst2Proto, "dst2proto_lg", False)
)


async def main():
    config = init()
    clickhouse_config = config["clickhouse_connection"]
    tarantool_config = config["tarantool_connection"]
    time_offset = config["time_offset"]
    updating_interval = config["updating_interval"]

    flows = list()
    for ch_query_model, tn_space_name, attack in pairs:
        clickhouse_config["result_model"] = ch_query_model
        ch_query = ClickhouseQuery(**clickhouse_config)

        tarantool_config["space_name"] = tn_space_name
        tn_upsert = TarantoolUpsert(**tarantool_config)

        flow = Flow(ch_query, tn_upsert)

        clickhouse_context = {
            "time_offset": time_offset,
            "under_attack": attack
        }

        flows.append((flow, clickhouse_context))

    work_queue = asyncio.Queue()

    while True:
        start_time = current_time()
        print(f"{current_time_str()}::main: start processing")
        for flow in flows:
            await work_queue.put(flow)

        tasks = [asyncio.create_task(task(name, work_queue)) for name in ("First", "Second", "Third")]
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
