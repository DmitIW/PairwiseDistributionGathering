from infi.clickhouse_orm import (
    UInt32Field, UInt16Field, Memory, Model
)

from clickhouse_agent.query import (
    QueryAttributed
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


class PairWiseModel(Model):
    engine = Memory()

    @staticmethod
    def table(database_name: str, attack: bool) -> str:
        table = "accept"
        if attack:
            table = "drop"
        return f"{database_name}.{table}"

    def row(self) -> tuple:
        return tuple(self.__dict__.values())[:-1]

    def _get_column_name(self, n: int) -> str:
        return tuple(self.__dict__.keys())[n]

    def first_column(self) -> str:
        return self._get_column_name(0)

    def second_column(self) -> str:
        return self._get_column_name(1)

    def counter_column(self) -> str:
        return self._get_column_name(2)


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
