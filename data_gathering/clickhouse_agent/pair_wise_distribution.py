from infi.clickhouse_orm import (
    UInt32Field, UInt16Field
)

from data_gathering.clickhouse_agent.base.query import (
    QueryAttributed
)

from data_gathering.clickhouse_agent.base.result_model import (
    ResultModel
)


class PairWiseQuery(QueryAttributed):
    q = """select {first_column}, {second_column}, count() as total_count
from {table}
prewhere timestamp > toUnixTimestamp(now()) - {time_offset}
where {first_column} != 0
  and {second_column} != 0
group by {first_column}, {second_column}
union all
select {first_column}, 0 as {second_column}, count() as total_count
from {table}
prewhere timestamp > toUnixTimestamp(now()) - {time_offset}
where {first_column} != 0
group by {first_column}, {second_column}
union all
select 0, {table}.{first_column}, toUInt64(round(count() / uniq({table}.{second_column}))) as total_count
from {table}
    prewhere timestamp > toUnixTimestamp(now()) - {time_offset}
where {table}.{first_column} != 0
group by {table}.{first_column}"""


class PairWiseModel(ResultModel):
    bind_query = PairWiseQuery

    @staticmethod
    def table(database_name: str, attack: bool) -> str:
        table = "accept"
        if attack:
            table = "drop"
        return f"{database_name}.{table}"

    def _get_column_name(self, n: int) -> str:
        return tuple(self.__dict__.keys())[n]

    def construct_query(self, time_offset: int, database_name: str,
                        under_attack: bool) -> str:
        query = self.bind_query()

        query.time_offset = str(time_offset)
        query.table = self.table(database_name, under_attack)
        query.first_column = self._get_column_name(0)
        query.second_column = self._get_column_name(1)

        return query.set_arguments().query()


class Src2Dst(PairWiseModel):
    src_port = UInt16Field()
    dst_port = UInt16Field()
    total_count = UInt32Field()


class Dst2Src(PairWiseModel):
    dst_port = UInt16Field()
    src_port = UInt16Field()
    total_count = UInt32Field()


class Dst2Proto(PairWiseModel):
    dst_port = UInt16Field()
    proto = UInt16Field()
    total_count = UInt32Field()
