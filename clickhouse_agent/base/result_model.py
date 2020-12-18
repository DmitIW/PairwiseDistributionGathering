from infi.clickhouse_orm import (
    Memory, Model
)


class ResultModel(Model):
    bind_query = None
    engine = Memory()

    def row(self) -> tuple:
        return tuple(self.__dict__.values())[:-1]

    def construct_query(self, **kwargs) -> str:
        raise NotImplementedError


def make_query(result_model: type(ResultModel), **kwargs) -> str:
    return result_model().construct_query(**kwargs)
