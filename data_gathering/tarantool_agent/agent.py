import aiotarantool
from tarantool import error

from typing import Union, Tuple, Generator, Any, Iterator

from data_gathering.base.connection import (
    AConnector, ACloseable, AConnection
)

from data_gathering.base.time_utility import (
    current_time
)


async def create_connection(url: str, port: int, **kwargs) -> aiotarantool.Connection:
    return aiotarantool.connect(host=url, port=port, **kwargs)


class TarantoolAgent(AConnector, ACloseable):
    async def start(self, url: str, port: int, **kwargs):
        self.conn = await create_connection(url, port, **kwargs)
        return self

    async def stop(self):
        await self.conn.close()
        self.conn = None

    def __init__(self):
        AConnector.__init__(self, self.start)
        ACloseable.__init__(self, self.stop)

        self.conn = None

    async def upsert(self, space_name: Union[str, int], data: Tuple[Union[str, int]], expiration_time: int):
        exp_time = current_time() + expiration_time
        new_value = data[-1]
        data = (*data, exp_time)

        try:
            await self.conn.upsert(space_name, data,
                                   [("=", 2, new_value), ("=", 3, exp_time)])
        except error.DatabaseError as e:
            print(f"TarantoolAgent:upsert:error {str(e)}")


class TarantoolUpsert:
    def __init__(self, url: str, port: int, space_name: str, expiration_time: int, **kwargs):
        self.tarantool_agent = AConnection(TarantoolAgent(), url=url, port=port, **kwargs)
        self.space_name = space_name
        self.expiration_time = expiration_time

    async def _upsert(self, connection: TarantoolAgent, data: Tuple[Union[str, int]]):
        await connection.upsert(space_name=self.space_name, data=data, expiration_time=self.expiration_time)

    async def upsert(self, data: Tuple[Union[str, int]]):
        async with self.tarantool_agent as conn:
            await self._upsert(conn, data)

    async def upsert_from(self, data_generator: Union[Generator[Tuple[Union[str, int]], Any, None], Iterator]):
        async with self.tarantool_agent as conn:
            for data in data_generator:
                await self._upsert(conn, data)


class TarantoolAgentCL(TarantoolAgent):
    async def upsert(self, space_name: Union[str, int], data: Tuple[Union[str, int]], expiration_time: int):
        exp_time = current_time() + expiration_time
        new_value = data[-2]
        slc = data[-1]
        data = (*data, exp_time)

        try:
            await self.conn.upsert(space_name, data,
                                   [("=", 1, new_value),
                                    ("=", 2, slc), ("=", 3, exp_time)])
        except error.DatabaseError as e:
            print(f"TarantoolAgent:upsert:error {str(e)}")


class TarantoolUpsertCL(TarantoolUpsert):
    def __init__(self, url: str, port: int, space_name: str, expiration_time: int, **kwargs):
        super(TarantoolUpsertCL, self).__init__(url, port, space_name, expiration_time, **kwargs)
        self.tarantool_agent = AConnection(TarantoolAgentCL(), url=url, port=port, **kwargs)
