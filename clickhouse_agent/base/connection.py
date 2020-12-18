from typing import NoReturn, Callable, Any

from functools import wraps


class Connected(object):
    def __init__(self):
        self.connected = False

    def is_connected(self) -> bool:
        return self.connected

    def connect(self) -> NoReturn:
        self.connected = True

    def disconnect(self) -> NoReturn:
        self.connected = False


def is_connected(method):
    @wraps(method)
    def _is_connected(self: Connected, *args, **kwargs):
        if self.is_connected():
            return method(self, *args, **kwargs)
        raise RuntimeError("Connection was not established")

    return _is_connected


def is_disconnected(method):
    @wraps(method)
    def _is_disconnected(self: Connected, *args, **kwargs):
        if not self.is_connected():
            return method(self, *args, **kwargs)
        raise RuntimeError("Connection was established already")

    return _is_disconnected


def connect(method):
    @wraps(method)
    def _connect(self: Connected, *args, **kwargs):
        result = method(self, *args, **kwargs)
        self.connect()
        return result

    return _connect


def disconnect(method):
    @wraps(method)
    def _connect(self: Connected, *args, **kwargs):
        result = method(self, *args, **kwargs)
        self.disconnect()
        return result

    return _connect


class Connector:
    def __init__(self, connection_method: Any):
        self.connection_method = connection_method

    def __call__(self, *args, **kwargs):
        return self.connection_method(*args, **kwargs)


class AConnector(Connector):
    async def __call__(self, *args, **kwargs):
        result = await self.connection_method(*args, **kwargs)
        return result


class Closeable:
    def __init__(self, close_method: Any, **kwargs):
        self.close_method = close_method
        self.kwargs = kwargs

    def close(self):
        self.close_method(**self.kwargs)


class ACloseable(Closeable):
    async def close(self):
        await self.close_method(**self.kwargs)



class Connection(Connected):
    def __init__(self, connector: Callable, *args, **kwargs):
        super(Connection, self).__init__()
        self._connection = None
        self._connector = connector
        self._args = args
        self._kwargs = kwargs

    @is_disconnected
    @connect
    def start(self):
        self._connection = self._connector(*self._args, **self._kwargs)
        return self._connection

    @is_connected
    @disconnect
    def stop(self):
        self._connection.close()
        self._connection = None
        return self

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            raise exc_type(exc_val)
        return self.stop()


class AConnection(Connection):
    @is_disconnected
    @connect
    async def start(self):
        self._connection = await self._connector(*self._args, **self._kwargs)
        return self._connection

    @is_connected
    @disconnect
    async def stop(self):
        await self._connection.close()
        self._connection = None
        return self

    async def __aenter__(self):
        result = await self.start()
        return result

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        result = await self.stop()
        return result
