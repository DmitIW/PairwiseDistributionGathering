import asyncio

from data_gathering.base import current_time_str
from data_gathering.config import init
from data_gathering.union import Flow
from data_gathering.clickhouse_agent import ClickhouseQuery
from data_gathering.tarantool_agent import TarantoolUpsert
from data_gathering.async_task import task

from data_gathering.clickhouse_agent.pair_wise_distribution import Src2Dst, Dst2Src, Dst2Proto


async def main():
    config = init()
    clickhouse_config = config["clickhouse_connection"]
    tarantool_config = config["tarantool_connection"]

    flows = list()

    clickhouse_config["result_model"] = Src2Dst
    ch_query = ClickhouseQuery(**clickhouse_config)

    tarantool_config["space_name"] = "src2dst_at"
    tn_upsert = TarantoolUpsert(**tarantool_config)

    clickhouse_context = {
        "time_offset": 60 * 1,
        "under_attack": True
    }

    flow = Flow(ch_query, tn_upsert)

    flows.append(flow)

    work_queue = asyncio.Queue()
    for f in flows:
        await work_queue.put((f, clickhouse_context))

    await asyncio.gather(
        asyncio.create_task(task("First", work_queue))
    )


if __name__ == '__main__':
    print(f"{current_time_str()}::Pairwise distribution start gathering")
    asyncio.run(main())
    print(f"{current_time_str()}::Pairwise distribution stop gathering")
