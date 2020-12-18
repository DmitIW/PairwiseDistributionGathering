from typing import Generator, Any

from infi.clickhouse_orm import (
    Database, Model
)

from clickhouse_agent.connection import (
    Connector, Closeable
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
