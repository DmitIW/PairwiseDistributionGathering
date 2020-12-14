from typing import Tuple, NoReturn, Union, Any
from tarantool import Connection, connect, error

from processing.storage import Storage

from utility.time import current_time
from utility.connector import if_connected, if_disconnected, Connector

from warnings import warn


def create_connection(url: str, port: int, **kwargs) -> Connection:
    return connect(host=url, port=port, **kwargs)


def insert_query(connection: Connection, space_name: Union[int, str],
                 data: Tuple[Union[str, int]], expiration_time: int) -> NoReturn:
    time_now = current_time()
    data = (*data, time_now + expiration_time)
    try:
        connection.upsert(space_name, data, [("=", 3, data[-2]), ("=", 4, data[-1])])
    except error.DatabaseError as e:
        warn(f"TarantoolAgent:insert:error {str(e)}", DeprecationWarning, stacklevel=1)


class SpaceWriter(Storage, Connector):
    def _my_space_name(self) -> str:
        raise NotImplementedError

    def __init__(self, url: str, port: int, expiration_time: int,
                 **kwargs):
        self.connection_context = {
            "url": url,
            "port": port,
            **kwargs
        }
        self.connection = None
        self.expiration_time = expiration_time

    @if_disconnected
    def connect(self):
        self.connection = create_connection(**self.connection_context)
        return self

    @if_connected
    def close(self):
        self.connection.close()
        self.connection = None
        return self

    @if_connected
    def insert(self, data: Tuple[Union[int, str]]) -> NoReturn:
        insert_query(self.connection, self._my_space_name(), data, self.expiration_time)

    def store(self, data: Any) -> NoReturn:
        self.insert(data)

    def connected(self) -> bool:
        if self.connection is None:
            return False
        return True

    def __enter__(self):
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class Src2DstAttack(SpaceWriter):
    def _my_space_name(self) -> str:
        return "src2dst_at"


class Src2DstLegal(SpaceWriter):
    def _my_space_name(self) -> str:
        return "src2dst_lg"


class Dst2SrcAttack(SpaceWriter):
    def _my_space_name(self) -> str:
        return "dst2src_at"


class Dst2SrcLegal(SpaceWriter):
    def _my_space_name(self) -> str:
        return "dst2src_lg"


class Dst2ProtoAttack(SpaceWriter):
    def _my_space_name(self) -> str:
        return "dst2proto_at"


class Dst2ProtoLegal(SpaceWriter):
    def _my_space_name(self) -> str:
        return "dst2proto_lg"
