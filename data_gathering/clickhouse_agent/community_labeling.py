from typing import Generator, Any

from infi.clickhouse_orm import (
    UInt32Field
)

from data_gathering.clickhouse_agent.base.query import (
    QueryAttributed
)

from data_gathering.clickhouse_agent.base.result_model import (
    ResultModel
)

from data_gathering.clickhouse_agent.agent import (
    ClickhouseQuery
)
from data_gathering.community_labeling import (
    comm_labelling
)


class CommunityLabelingQuery(QueryAttributed):
    q = """SELECT {first_column}, {second_column}, count() AS weight
FROM {table}
WHERE timestamp > toUnixTimestamp(now()) - {time_offset}
GROUP BY {first_column}, {second_column}"""


class CommunityLabelModel(ResultModel):
    bind_query = CommunityLabelingQuery

    @staticmethod
    def table(database_name: str) -> str:
        return f"{database_name}.common"

    def _get_column_name(self, n: int) -> str:
        return tuple(self.__dict__.keys())[n]

    def construct_query(self, database_name: str, time_offset: int) -> str:
        query = self.bind_query()

        query.time_offset = str(time_offset)
        query.table = self.table(database_name)
        query.first_column = self._get_column_name(0)
        query.second_column = self._get_column_name(1)

        return query.set_arguments().query()


class S2DComm(CommunityLabelModel):
    src_net = UInt32Field()
    dst_net = UInt32Field()
    weight = UInt32Field()


class CHCommLabQuery(ClickhouseQuery):
    def execute(self) -> Generator[Any, Any, None]:
        yield from comm_labelling(super(CHCommLabQuery, self).execute())
