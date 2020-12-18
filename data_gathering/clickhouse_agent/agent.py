from typing import Generator, Any

from infi.clickhouse_orm import (
    Database, Model
)

from data_gathering.base.connection import (
    Connector, Closeable, Connection
)

from data_gathering.clickhouse_agent.base.result_model import (
    ResultModel, make_query
)


def create_connection(database: str, url: str, **kwargs) -> Database:
    return Database(database, url, readonly=True, **kwargs)


class ClickhouseAgent(Connector, Closeable):
    def start(self, database: str, url: str, **kwargs):
        self.conn = create_connection(database, url, **kwargs)
        return self

    def stop(self):
        del self.conn
        self.conn = None

    def __init__(self):
        Connector.__init__(self, self.start)
        Closeable.__init__(self, self.stop)

        self.conn = None

    def select(self, query: str, result_model: Model) -> Generator[Any, Any, None]:
        yield from self.conn.select(query, model_class=result_model)


class ClickhouseQuery:
    def __init__(self, database: str, url: str, result_model: type(ResultModel), **connection_args):
        self.database = database
        self.clickhouse_agent = Connection(ClickhouseAgent(), database=database, url=url, **connection_args)
        self.clickhouse_query_model = result_model
        self.query = None

    def prepare_query(self, **query_args):
        query_args["database_name"] = self.database
        self.query = make_query(self.clickhouse_query_model, **query_args)
        return self

    def execute(self) -> Generator[Any, Any, None]:
        with self.clickhouse_agent as conn:
            yield from conn.select(self.query, self.clickhouse_query_model)
