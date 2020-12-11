import os


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


def get_tarantool_url(variable_name: str = None, default_value: str = None) -> str:
    if variable_name is None:
        variable_name = "TARANTOOL_HOST"
    if default_value is None:
        default_value = "localhost"

    return os.environ.get(variable_name, default_value)


def get_tarantool_port(variable_name: str = None, default_value: int = None) -> int:
    if variable_name is None:
        variable_name = "TARANTOOL_PORT"
    if default_value is None:
        default_value = 3301

    return os.environ.get(variable_name, default_value)


DEFAULT_TIME_AMOUNT = 60 * 60 * 1


def get_time_offset(variable_name: str = None, default_value: int = None) -> int:
    if variable_name is None:
        variable_name = "GATHERING_TIME_OFFSET"
    if default_value is None:
        global DEFAULT_TIME_AMOUNT
        default_value = DEFAULT_TIME_AMOUNT

    return os.environ.get(variable_name, default_value)


def get_expiration_time(variable_name: str = None, default_value: int = None) -> int:
    if variable_name is None:
        variable_name = "FACT_EXPIRATION_TIME"
    if default_value is None:
        global DEFAULT_TIME_AMOUNT
        default_value = DEFAULT_TIME_AMOUNT

    return os.environ.get(variable_name, default_value)
