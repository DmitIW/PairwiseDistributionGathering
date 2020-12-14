from flow.clickhouse_agent import (
    ClickhouseAgent
)
from processing.storage import (
    Storage
)
from typing import Any, Dict, Tuple


class Flow:
    def __init__(self, clickhouse_agent: ClickhouseAgent, target_storage: Storage, **kwargs):
        self.ch_agent = clickhouse_agent
        self.target_storage = target_storage
        self.args = kwargs

    def get_source(self) -> ClickhouseAgent:
        return self.ch_agent

    def get_destination(self) -> Storage:
        return self.target_storage

    def get_argument(self, name: str, default_value: Any = None):
        if name in self.args:
            return self.args[name]
        return default_value

    def get_arguments(self) -> Dict[str, Any]:
        return self.args

    def __enter__(self) -> Tuple[ClickhouseAgent, Storage]:
        source = self.ch_agent.__enter__()
        destination = self.target_storage.__enter__()
        return source, destination

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.ch_agent.__exit__(exc_type, exc_val, exc_tb)
        self.target_storage.__exit__(exc_type, exc_val, exc_tb)


def create_flow(flow_from: ClickhouseAgent, flow_to: Storage, **kwargs) -> Flow:
    return Flow(flow_from, flow_to, **kwargs)
