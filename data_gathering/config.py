import os

from data_gathering.utility.constraints import (
    return_int
)

from typing import Dict, Any


def get_clickhouse_url(variable_name: str = None, default_value: str = None) -> str:
    if variable_name is None:
        variable_name = "CLICKHOUSE_URL"
    if default_value is None:
        default_value = "http://localhost:8123"

    return os.environ.get(variable_name, default_value)


def get_clickhouse_db(variable_name: str = None, default_value: str = None) -> str:
    if variable_name is None:
        variable_name = "CLICKHOUSE_DB"
    if default_value is None:
        default_value = ""

    return os.environ.get(variable_name, default_value)


def get_clickhouse_user(variable_name: str = None, default_value: str = None) -> str:
    if variable_name is None:
        variable_name = "CLICKHOUSE_USER"
    if default_value is None:
        default_value = "default"

    return os.environ.get(variable_name, default_value)


def get_tarantool_url(variable_name: str = None, default_value: str = None) -> str:
    if variable_name is None:
        variable_name = "TARANTOOL_HOST"
    if default_value is None:
        default_value = "localhost"

    return os.environ.get(variable_name, default_value)


@return_int
def get_tarantool_port(variable_name: str = None, default_value: int = None) -> int:
    if variable_name is None:
        variable_name = "TARANTOOL_PORT"
    if default_value is None:
        default_value = 3301

    return os.environ.get(variable_name, default_value)


DEFAULT_TIME_AMOUNT = 60 * 60 * 1


@return_int
def get_time_offset(variable_name: str = None, default_value: int = None) -> int:
    if variable_name is None:
        variable_name = "GATHERING_TIME_OFFSET"
    if default_value is None:
        global DEFAULT_TIME_AMOUNT
        default_value = DEFAULT_TIME_AMOUNT

    return os.environ.get(variable_name, default_value)


@return_int
def get_expiration_time(variable_name: str = None, default_value: int = None) -> int:
    if variable_name is None:
        variable_name = "FACT_EXPIRATION_TIME"
    if default_value is None:
        global DEFAULT_TIME_AMOUNT
        default_value = DEFAULT_TIME_AMOUNT

    return os.environ.get(variable_name, default_value)


@return_int
def get_updating_interval(variable_name: str = None, default_value: int = None) -> int:
    if variable_name is None:
        variable_name = "UPDATING_INTERVAL"
    if default_value is None:
        global DEFAULT_TIME_AMOUNT
        default_value = DEFAULT_TIME_AMOUNT

    return os.environ.get(variable_name, default_value)


def init() -> Dict[str, Any]:
    return {
        "clickhouse_connection":
            {
                "database": get_clickhouse_db(),
                "url": get_clickhouse_url(),
                "username": get_clickhouse_user()
            },
        "tarantool_connection":
            {
                "url": get_tarantool_url(),
                "port": get_tarantool_port(),
                "expiration_time": get_expiration_time()
            },
        "time_offset": get_time_offset(),
        "updating_interval": get_updating_interval(),
    }
