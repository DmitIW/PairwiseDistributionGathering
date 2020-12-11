from flow.clickhouse_agent import (
    SelectQueryResult
)
from processing.storage import (
    Storage
)
from typing import Any, Dict


class Flow:
    def __init__(self, fr: SelectQueryResult, to: Storage, **kwargs):
        self.fr = fr
        self.to = to
        self.args = kwargs

    def get_source(self) -> SelectQueryResult:
        return self.fr

    def get_destination(self) -> Storage:
        return self.to

    def get_argument(self, name: str, default_value: Any = None):
        if name in self.args:
            return self.args[name]
        return default_value

    def get_arguments(self) -> Dict[str, Any]:
        return self.args


def create_flow(flow_from: SelectQueryResult, flow_to: Storage, **kwargs) -> Flow:
    return Flow(flow_from, flow_to, **kwargs)
