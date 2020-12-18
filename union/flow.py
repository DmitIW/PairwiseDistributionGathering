from clickhouse_agent import ClickhouseQuery
from tarantool_agent import TarantoolUpsert


class Flow:
    def __init__(self, clickhouse_side: ClickhouseQuery, tarantool_side: TarantoolUpsert):
        self.clickhouse = clickhouse_side
        self.tarantool = tarantool_side

    def prepare_clickhouse_query(self, **kwargs):
        self.clickhouse = self.clickhouse.prepare_query(**kwargs)
        return self

    async def start(self):
        with self.clickhouse as clickhouse_conn, self.tarantool as tarantool_conn:
            for row in map(lambda x: x.row(), clickhouse_conn.execute()):
                await tarantool_conn.upsert(row)
