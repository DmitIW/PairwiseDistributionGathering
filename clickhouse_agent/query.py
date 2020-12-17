import string

from typing import NoReturn, Any, Generator
from functools import wraps


class QuerySetting(object):
    """
    Проверка, выставлены ли аргументы для запроса
    """

    def __init__(self):
        self._ready = False

    def is_ready(self) -> bool:
        return self._ready

    def ready(self) -> NoReturn:
        self._ready = True


def is_ready(method):
    @wraps(method)
    def _is_ready(self: QuerySetting, *args, **kwargs):
        if self.is_ready():
            return method(self, *args, **kwargs)
        raise RuntimeError("Query is not ready for use")

    return _is_ready


def ready(method):
    @wraps(method)
    def _ready(self: QuerySetting, *args, **kwargs):
        result = method(self, *args, **kwargs)
        self.ready()
        return result

    return _ready


def is_query(method):
    @wraps(method)
    def _is_query(self, q: str, *args, **kwargs):
        if q is None:
            raise TypeError("Query should be set in variable q")
        if type(q) != str:
            raise TypeError("Query should be string")
        return method(self, q, *args, **kwargs)

    return _is_query


class QueryBase(QuerySetting):
    """
    Формирование запроса
    """

    @is_query
    def __init__(self, q: str):
        super(QueryBase, self).__init__()
        self._query = q
        self._prepared_query = None

    @is_ready
    def query(self) -> str:
        return self._prepared_query

    @ready
    def set_arguments(self, *args, **kwargs):
        self._prepared_query = self._query.format(*args, **kwargs)
        return self


class Query(QueryBase):
    """
    Интерфейс для хранения зпроса с аргументами
    """
    q = None

    def __init__(self):
        super(Query, self).__init__(self.q)


class SimpleQuery(Query):
    """
    Интерфейс для хранения зпроса без аргументов
    """

    def __init__(self):
        super(SimpleQuery, self).__init__()
        self.set_arguments()


class QueryAttributed(Query):
    def __str__(self) -> str:
        result = f"Query: {self.q}"
        for attr, value in map(lambda x: x if x[1] is not None else (x[0], "empty"), self._attributes()):
            result += f"\n{attr}: {value}"

        return result

    @is_query
    def __attributing(self, q: str) -> str:
        fmt = string.Formatter()

        for attribute in map(lambda x: x[1],
                             filter(lambda x: len(x) > 1 and x[1] is not None,
                                    fmt.parse(q))):
            self.attributes.add(attribute)
            setattr(self, attribute, None)

        return q

    def _attributes(self) -> Generator[Any, Any, None]:
        yield from filter(lambda x: x[0] in self.attributes, self.__dict__.items())

    def __init__(self):
        self.attributes = set()
        self.q = self.__attributing(self.q)
        super(QueryAttributed, self).__init__()

    def set_arguments(self, *args, **kwargs):
        for attr, value in self._attributes():
            if value is None:
                raise TypeError(f"Attribute {attr} is not set")
            if type(value) != str:
                raise TypeError(f"Attribute {attr} has wrong type: {type(value)}")
            kwargs[attr] = value
        super(QueryAttributed, self).set_arguments(*args, **kwargs)
