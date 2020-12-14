from functools import wraps


class Connector:
    def connected(self) -> bool:
        raise NotImplementedError


def if_connected(method):
    @wraps(method)
    def _check_connection(self: Connector, *args, **kwargs):
        if self.connected():
            return method(self, *args, **kwargs)
        raise RuntimeError("Connection was not established")

    return _check_connection


def if_disconnected(method):
    @wraps(method)
    def _check_connection(self: Connector, *args, **kwargs):
        if self.connected():
            raise RuntimeError("Connection is established already")
        return method(self, *args, **kwargs)

    return _check_connection
