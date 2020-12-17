from clickhouse_agent.query import (
    QueryAttributed
)


class PairWiseQuery(QueryAttributed):
    q = """select {first_column}, {second_column}, count() as total_count
from {table}
prewhere timestamp > toUnixTimestamp(now()) - {time_offset}
where {first_column} != 0
  and {second_column} != 0
group by {first_column}, {second_column}
union all
select {first_column}, 0 as {second_column}, count() as total_count
from {table}
prewhere timestamp > toUnixTimestamp(now()) - {time_offset}
where {first_column} != 0
group by {first_column}, {second_column}
union all
select 0, {table}.{first_column}, toUInt64(round(count() / uniq({table}.{second_column}))) as total_count
from {table}
    prewhere timestamp > toUnixTimestamp(now()) - {time_offset}
where {table}.{first_column} != 0
group by {table}.{first_column}"""
