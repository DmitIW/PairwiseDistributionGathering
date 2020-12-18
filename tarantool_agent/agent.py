import aiotarantool
from tarantool import error

from typing import Union, Tuple

from base.connection import (
    AConnector, ACloseable, AConnection
)

from base.time_utility import (
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

    async def upsert(self, data: Tuple[Union[str, int]]):
        async with self.tarantool_agent as conn:
            await conn.upsert(space_name=self.space_name, data=data, expiration_time=self.expiration_time)
