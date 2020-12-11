from infi.clickhouse_orm import (
    Database, Model, StringField, UInt32Field, UInt16Field, Memory
)
from typing import Generator, Any, Union, Tuple


class PairwiseDistQuery:
    def __init__(self, first_column: str, second_column: str, target_db: str):
        self.first_column = first_column
        self.second_column = second_column
        self.target_db = target_db

    def pairwise_dist_select_query(self, time_offset: int, table: str) -> str:
        return f"""select {self.first_column}, {self.second_column}, count() as total_count
from {table}
prewhere timestamp > toUnixTimestamp(now()) - {time_offset}
where {self.first_column} != 0
  and {self.second_column} != 0
group by {self.first_column}, {self.second_column}"""

    def target_table(self, attack: bool) -> str:
        if attack:
            return f"{self.target_db}.drop"
        return f"{self.target_db}.accept"

    def query(self, time_offset: int, attack: bool) -> str:
        return self.pairwise_dist_select_query(time_offset, self.target_table(attack))


class SelectQueryResult:
    @staticmethod
    def query(**kwargs):
        raise NotImplementedError

    def row(self) -> Tuple:
        return tuple(self.__dict__.values())


class Src2Dst(Model, SelectQueryResult):
    src_port = UInt16Field()
    dst_port = UInt16Field()
    total_count = UInt32Field()

    engine = Memory()

    @staticmethod
    def query(time_offset: int, attack: bool) -> str:
        return PairwiseDistQuery("src_port", "dst_port", "pieflow").query(time_offset, attack)


class Dst2Src(Model, SelectQueryResult):
    dst_port = UInt16Field()
    src_port = UInt16Field()
    total_count = UInt32Field()

    engine = Memory()

    @staticmethod
    def query(time_offset: int, attack: bool) -> str:
        return PairwiseDistQuery("dst_port", "src_port", "pieflow").query(time_offset, attack)


class Dst2Proto(Model, SelectQueryResult):
    dst_port = UInt16Field()
    proto = UInt16Field()
    total_count = UInt32Field()

    engine = Memory()

    @staticmethod
    def query(time_offset: int, attack: bool) -> str:
        return PairwiseDistQuery("dst_port", "proto", "pieflow").query(time_offset, attack)


def exec_query(database: Database,
               data_model: Union[Model, SelectQueryResult], **kwargs) -> Generator[Any, Any, None]:
    yield from database.select(
        query=data_model.query(**kwargs),
        model_class=data_model
    )


def create_connection(database_name: str, url: str, **kwargs) -> Database:
    return Database(database_name, url, readonly=True, **kwargs)
